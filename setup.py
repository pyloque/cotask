# -*- coding: utf-8 -*-
import os
import sys

from setuptools import setup
from setuptools.command.test import test as TestCommand

f = open(os.path.join(os.path.dirname(__file__), 'version.txt'))
version = f.read().strip()
f.close()


class SetupTest(TestCommand):

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        try:
            import pytest
            import mockredis  # noqa
        except ImportError:
            return
        errno = pytest.main(self.test_args)
        sys.exit(errno)

setup(
    name="cotask",
    version=version,
    description="task scheduler based on tornado",
    author="qianwenpin",
    author_email="pyloque@gmail.com",
    license="MIT",
    packages=['cotask'],
    keywords=['tornado task scheduler'],
    cmdclass={'test': SetupTest})
