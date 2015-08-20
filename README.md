Tornado based task scheduler
===============================

间隔ticker
--------------

```
from cotask import Ticker, TaskCenter


class TestTicker(Ticker):

    name = 'test'
    period = 1

    def run(self):
        print 'test'


center = TaskCenter()
center.register_ticker(TestTicker())
center.start_with_options()
```

普通任务，redis队列
-------------------
```
from cotask import Task, Ticker, TaskCenter
from mockredis import mock_strict_redis_client as mock_client


redis = mock_client()


class TestTicker(Ticker):

    name = 'test'
    period = 1

    def run(self):
        redis.rpush('xxx', 'test')


class TestTask(Task):
    name = 'test'
    sleep_on_empty = 1

    def take(self):
        return redis.lpop('xxx')

    def handle(self, value):
        print value


center = TaskCenter()
center.register_ticker(TestTicker())
center.register_task(TestTask, 1)
center.start_with_options()
```

http异步调用
---------------------
```
from cotask import Task, Ticker, TaskCenter, HttpMixin
from tornado.gen import coroutine
from Queue import Queue, Empty


queue = Queue()


class TestTicker(Ticker):

    name = 'test'
    period = 0.5

    def run(self):
        queue.put_nowait('http://www.baidu.com/')


class TestTask(Task, HttpMixin):
    name = 'test'
    sleep_on_empty = 1

    def take(self):
        try:
            return queue.get_nowait()
        except Empty:
            pass

    @coroutine
    def handle(self, url):
        response = yield self.get(url)
        print response.code


center = TaskCenter()
center.register_ticker(TestTicker())
center.register_task(TestTask, 2)
center.start_with_options()
```

外部shell任务
---------------------------
```

from cotask import Task, Ticker, TaskCenter, ShellMixin
from tornado.gen import coroutine
from Queue import Queue, Empty


queue = Queue()


class TestTicker(Ticker):

    name = 'test'
    period = 0.5

    def run(self):
        queue.put_nowait('ttt')


class TestTask(Task, ShellMixin):
    name = 'test'
    sleep_on_empty = 1

    def take(self):
        try:
            return queue.get_nowait()
        except Empty:
            pass

    @coroutine
    def handle(self, url):
        command = "wc -l *.py | awk '{print $1}' | awk '{a+=$0}END{print a}'"
        result = yield self.execute_shell(command)
        print result


center = TaskCenter()
center.register_ticker(TestTicker())
center.register_task(TestTask, 2)
center.start_with_options()
```

线程池任务
----------------------
```
from cotask import Task, Ticker, TaskCenter, ExecutorMixin
from tornado.gen import coroutine
from Queue import Queue, Empty
import time


queue = Queue()


class TestTicker(Ticker):

    name = 'test'
    period = 0.5

    def run(self):
        queue.put_nowait('ttt')


class TestTask(Task, ExecutorMixin):
    name = 'test'
    sleep_on_empty = 1

    def take(self):
        try:
            return queue.get_nowait()
        except Empty:
            pass

    @coroutine
    def handle(self, url):
        result = yield self.run_async(self.long_run)
        print result

    def long_run(self):
        time.sleep(2)
        return 'awake'


center = TaskCenter()
center.register_ticker(TestTicker())
center.register_task(TestTask, 2)
center.start_with_options()
```

rabbitmq收发消息
--------------------
```

from pika import BlockingConnection, URLParameters
from cotask import Task, Ticker, TaskCenter, AmqpMixin


class TestTicker(Ticker, AmqpMixin):

    name = 'test'
    period = 0.5
    exchange_name = 'etest'
    exchange_type = 'topic'
    queue_name = 'qtest'
    routing_key = 'rtest'
    no_ack = True

    @property
    def connection(self):
        return BlockingConnection(URLParameters('amqp://guest:guest@localhost:5672'))

    def initialize(self):
        self.declare()

    def run(self):
        self.publish('xxxxxxxxxxxxxxxxx')


class TestTask(Task, AmqpMixin):
    name = 'test'
    sleep_on_empty = 1
    exchange_name = 'etest'
    exchange_type = 'topic'
    queue_name = 'qtest'
    routing_key = 'rtest'
    no_ack = True

    @property
    def connection(self):
        return BlockingConnection(URLParameters('amqp://guest:guest@localhost:5672'))

    def initialize(self):
        self.declare()

    def take(self):
        return self.pop()

    def handle(self, item):
        print item.body


center = TaskCenter()
center.register_ticker(TestTicker())
center.register_task(TestTask, 2)
center.start_with_options()
```
