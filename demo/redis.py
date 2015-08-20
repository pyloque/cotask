# pylint: disable=all
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
