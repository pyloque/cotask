# -*- coding: utf-8 -*-
# pylint: disable=star-args, invalid-name, no-member, no-self-use
# pylint: disable=unused-argument, attribute-defined-outside-init
# pylint: disable=protected-access, import-error

'''
异步任务核心基础
'''

import os
import toro
import signal
import logging
import subprocess
from functools import partial

from tornado import gen, ioloop, httpclient
from tornado.util import ObjectDict
from tornado.httputil import url_concat
from tornado.concurrent import is_future, run_on_executor
from concurrent.futures import ThreadPoolExecutor

from .helpers import Unique, cached_property, classproperty


class Task(object):
    '''
    通用任务基础类
    '''
    _stop_signal = object
    name = None
    queue_size = 1
    sleep_on_empty = 5
    sleep_on_error = 1

    def __init__(self):
        self.will_stop = False
        self.is_stopped = False
        self.initialize()

    @classproperty
    def stat(cls):
        '''
        任务状态字典
        '''
        if hasattr(cls, '_stat'):
            return cls._stat
        cls._stat = ObjectDict(
            consumed=0,
            produced=0,
            take_errors=0,
            handle_errors=0)
        return cls._stat

    @cached_property
    def q(self):
        '''
        任务量控制队列
        '''
        return toro.JoinableQueue(maxsize=self.queue_size)

    def initialize(self):
        '''
        这里初始化
        '''
        pass

    def set_stop(self):
        '''
        标记停止，任务在消息处理完后就退出
        '''
        self.will_stop = True

    @gen.coroutine
    def produce(self):
        '''
        生产消息
        '''
        while True:
            if self.will_stop:
                yield self.q.put(self._stop_signal)
                break
            item = None
            try:
                item = self.take()
            except Exception, ex:
                logging.error(
                    "task %s take error:%s", self.name, str(ex), exc_info=True)
                self.stat.take_errors += 1
            if item is None:
                yield gen.sleep(self.sleep_on_empty)
                continue
            yield self.q.put(item)
            self.stat.produced += 1

    @gen.coroutine
    def consume(self):
        '''
        消费消息
        '''
        while True:
            if self.q.empty() and self.will_stop:
                break
            item = yield self.q.get()
            if item is self._stop_signal:
                break
            ret = None
            try:
                ret = self.handle(item)
                if is_future(ret):
                    yield ret
            except Exception, ex:
                logging.error(
                    "task %s handle error:%s, msg=%s",
                    self.name, str(ex), repr(item), exc_info=True)
                self.stat.handle_errors += 1
                yield gen.sleep(self.sleep_on_error)
            self.on_consumed()
            self.q.task_done()
            self.stat.consumed += 1
        self.on_stop()
        self.is_stopped = True

    def on_stop(self):
        '''
        停止任务的回调
        '''
        pass

    def on_consumed(self):
        '''
        消化一个回调一下
        '''
        logging.info('task:%s consumed a message', self.name)

    def take(self):
        '''
        这里生产消息具体实现
        '''
        pass

    def handle(self, item):
        '''
        这里消化消息具体实现
        '''
        pass

    def start(self):
        '''
        启动任务
        '''
        self.produce()
        self.consume()


class HttpMixin(object):

    '''
    用于异步执行http调用的task
    '''

    def _request_async(
            self, method, url, args, body, headers):
        '''
        异步调用
        '''
        client = httpclient.AsyncHTTPClient()
        query_url = url_concat(url, args)
        request = httpclient.HTTPRequest(
            query_url, method, headers=headers, body=body)
        return client.fetch(request)

    @gen.coroutine
    def get(self, url, args=None, headers=None):
        '''
        GET请求
        '''
        response = yield self._request_async(
            'GET', url, args=args, body=None, headers=headers)
        raise gen.Return(response)

    @gen.coroutine
    def post(self, url, args=None, body=None, headers=None):
        '''
        POST请求
        '''
        response = yield self._request_async(
            'POST', url, args=args, body=body, headers=headers)
        raise gen.Return(response)

    @gen.coroutine
    def put(self, url, args=None, body=None, headers=None):
        '''
        PUT请求
        '''
        response = yield self._request_async(
            'PUT', url, args=args, body=body, headers=headers)
        raise gen.Return(response)

    @gen.coroutine
    def delete(self, url, args=None, headers=None):
        '''
        DELETE请求
        '''
        response = yield self._request_async(
            'DELETE', url, args=args, body=None, headers=headers)
        raise gen.Return(response)


class ShellMixin(object):

    '''
    包含执行外部shell命令的task
    '''

    @gen.coroutine
    def execute_shell(self, command):
        '''
        异步执行外部shell命令

        @gen.coroutine
        def digest(self, message):
            result = yield self.execute_shell('sleep 1;echo hello')
            print result.returncode
            print result.output

        '''
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
        pipe = process.stdout
        shell_result = ObjectDict()

        def handle_events(shell_result, io_loop, fd, events):
            '''
            ioloop异步回调
            '''
            io_loop.remove_handler(fd)
            output = ''.join(pipe)
            process.wait()
            shell_result['returncode'] = process.returncode
            shell_result['output'] = output
            event.set()

        event = toro.Event()
        self.io_loop.add_handler(
            pipe.fileno(),
            partial(handle_events, shell_result, self.io_loop),
            self.io_loop.READ)
        yield event.wait()
        raise gen.Return(shell_result)

    @property
    def io_loop(self):
        '''
        当前的ioloop
        '''
        return ioloop.IOLoop.current()


class ExecutorMixin(object):
    '''
    线程池执行异步操作
    '''

    @cached_property
    def executor(self):
        '''
        使用全局线程池
        '''
        return TaskCenter().global_executor

    @gen.coroutine
    def run_async(self, block_func):
        '''
        就这么调用
        @gen.coroutine
        def digest(self, message):
            query_func = self.buildQueryFunc()
            entities = yield self.run_async(query_func)
            print len(entities)
        '''
        value = yield self._execute(block_func)
        raise gen.Return(value)

    @run_on_executor
    def _execute(self, block_func):
        '''
        异步执行
        '''
        return block_func()


class _TripleItem(object):
    '''
    amqp消息体
    '''

    def __init__(self, delivery, properties, body):
        self.delivery = delivery
        self.properties = properties
        self.body = body


class AmqpMixin(object):
    '''
    Rabbitmq 任务mixin
    '''
    exchange_name = None
    exchange_type = None
    queue_name = None
    routing_key = None
    no_ack = True

    @property
    def connection(self):
        '''
        rabbitmq 连接
        '''
        pass

    def declare(self):
        '''
        连接rabbitmq-server，声明exchange，queue
        '''
        self.channel = self.connection.channel()
        self.channel.exchange_declare(self.exchange_name, self.exchange_type)
        self.channel.queue_declare(self.queue_name)
        self.channel.queue_bind(
            self.queue_name, self.exchange_name, self.routing_key)

    def pop(self):
        '''
        从队列里拿一条出来
        '''
        triple = self.channel.basic_get(self.queue_name, no_ack=self.no_ack)
        if triple[0]:
            return _TripleItem(*triple)

    def publish(self, body, properties=None):
        '''
        发消息
        '''
        self.channel.basic_publish(
            self.exchange_name, self.routing_key, body, properties=properties)

    def ack(self, triple):
        '''
        确认消息
        '''
        self.channel.basic_ack(triple.delivery.delivery_tag)


class Ticker(Unique):
    '''
    周期循环触发器基础类
    '''
    period = 30
    name = None

    def run(self):
        '''
        时间到了做要做的事
        '''
        pass


class IdleTask(Task):
    '''
    空任务
    '''
    name = '_idle'

    def take(self):
        return True

    @gen.coroutine
    def handle(self, item):
        yield gen.sleep(0.1)

    def on_consumed(self):
        pass


class TaskCenter(Unique):

    '''
    任务注册中心
    '''
    shutdown_duration = 60  # 最多30s用于停机处理
    executor_size = 10
    executors = {}

    def initialize(self):
        self.all_tasks = {}
        self.tasks_cls = {}
        self.all_tickers = {}
        self.is_stopping = False
        self.quiter = None
        self.shutdown_start = None
        self.tasks_cls[IdleTask.name] = (IdleTask, 1, False)

    @cached_property
    def global_executor(self):
        '''
        全局线程池
        '''
        return self.allocate_executor('_global', self.executor_size)

    def allocate_executor(self, executor_name, executor_size):
        '''
        分配独立线程池
        '''
        if executor_name in self.executors:
            return self.executors[executor_name]
        executor = ThreadPoolExecutor(executor_size)
        self.executors[executor_name] = executor
        return executor

    def register_task(self, task_class, task_num, standalone=False):
        '''
        注册任务
        @param standalone 独立启动 必须通过传递-tasks参数进行启动
        '''
        self.tasks_cls[task_class.name] = (task_class, task_num, standalone)
        return self

    def register_ticker(self, ticker):
        '''
        注册定时器
        '''
        callback = ioloop.PeriodicCallback(ticker.run, ticker.period * 1000)
        self.all_tickers[ticker.name] = callback
        return self

    def start_with_options(self):
        '''
        从命令行参数启动
        '''
        from tornado.options import options, define
        define('includes', default='', help='specify tasks and tickers to run')
        define('excludes', default='',
               help='specify tasks and tickers not to run')
        options.parse_command_line()
        tasks = {}
        tickers = []
        if options.includes:
            tps = options.includes.split(',')
            for tp in tps:
                pair = tp.split(':')
                if len(pair) == 2:
                    tasks[pair[0]] = int(pair[1])
                else:
                    tickers.append(pair[0])
            tasks[IdleTask.name] = 1
        else:
            tasks = {
                name: num
                for name, (task_cls, num, standalone)
                in self.tasks_cls.items()
                if not standalone
            }
            tickers = self.all_tickers.keys()
        if options.excludes:
            tps = options.excludes.split(',')
            for tp in tps:
                tasks.pop(tp, None)
                if tp in tickers:
                    tickers.remove(tp)
        self.start_all(tasks, tickers)

    def start_all(self, tasks, tickers):
        '''
        初始化信号处理，加入退出钩子
        '''
        logging.warn('Attention! worker is starting...')
        self.start_tasks(tasks)
        self.start_tickers(tickers)
        signal.signal(signal.SIGINT, self.stop_all)
        signal.signal(signal.SIGTERM, self.stop_all)
        # 向进程发送USR1信号，输出任务状态信息
        signal.signal(signal.SIGUSR1, self.print_stat)
        ioloop.IOLoop.current().start()

    def start_tasks(self, tasks):
        '''
        启动指定任务
        初始化信号处理，加入退出钩子
        '''
        if not tasks:
            tasks = {
                name: num
                for name, (task_cls, num, standalone)
                in self.tasks_cls.items()
                if not standalone
            }
        for name, num in tasks.items():
            task_cls = self.tasks_cls[name][0]
            tasks = [task_cls() for _ in range(num)]
            self.all_tasks[name] = tasks
            for task in tasks:
                task.start()

    def start_tickers(self, tickers):
        '''
        启动触发器
        '''
        if not tickers:
            tickers = self.all_tickers.keys()
        for name in tickers:
            self.all_tickers[name].start()

    def stop_tickers(self):
        '''
        停止触发器
        '''
        for ticker in self.all_tickers.values():
            ticker.stop()

    def print_stat(self, signum, frame):
        '''
        控制台打印任务状态消息
        '''
        from pprint import pformat
        logging.warn(pformat(self.collect_stat()))

    def collect_stat(self, task_name=None):
        '''
        收集任务状态数据
        '''
        result = {}
        for name, tasks in self.all_tasks.items():
            result[name] = stat = self.tasks_cls[name][0].stat
            stat.task_num = len(tasks)
        if task_name:
            return result.get(task_name)
        else:
            return result

    def stop_all(self, signum, frame):
        '''
        停止任务、触发器
        '''
        if self.is_stopping:
            return
        self.is_stopping = True
        if not self.quiter:
            self.quiter = ioloop.PeriodicCallback(self.try_exit, 500)
            self.quiter.start()
            signal.signal(signal.SIGALRM, self.killme)
            signal.alarm(self.shutdown_duration)
        self.stop_tickers()
        for tasks in self.all_tasks.values():
            for task in tasks:
                task.set_stop()

    def try_exit(self):
        '''
        停止服务器
        '''
        if not self.is_stopping:
            return
        logging.warn('OMG! worker is trying to exit...')
        for tasks in self.all_tasks.values():
            for task in tasks:
                if not task.is_stopped:
                    return
        if self.quiter:
            self.quiter.stop()
        for executor in self.executors.values():
            executor.shutdown(wait=True)
        ioloop.IOLoop.current().stop()
        logging.warn('Oops! worker exited......')

    def killme(self, signum, frame):
        '''
        莫名其妙进程停不掉时，强制退出
        '''
        logging.error('Oops! force exited using alarm signal......')
        os._exit(0)
