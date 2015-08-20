# pylint: disable=all
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
