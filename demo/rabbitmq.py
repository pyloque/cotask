# pylint: disable=all
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
