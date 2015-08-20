# pylint: disable=all
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
