# pylint: disable=all
from cotask import Ticker, TaskCenter


class TestTicker(Ticker):

    name = 'test'
    period = 1

    def run(self):
        print 'test'


center = TaskCenter()
center.register_ticker(TestTicker())
center.start_with_options()
