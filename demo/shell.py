# pylint: disable=all
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
