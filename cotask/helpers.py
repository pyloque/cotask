# -*- coding: utf-8 -*-
# pylint: disable=super-init-not-called, invalid-name, redefined-builtin

'''
工具助手类
'''


class cached_property(property):
    '''
    缓存实例属性
    '''
    _missing = object()

    def __init__(self, func, name=None, doc=None):
        self.__name__ = name or func.__name__
        self.__module__ = func.__module__
        self.__doc__ = doc or func.__doc__
        self.func = func

    def __set__(self, obj, value):
        obj.__dict__[self.__name__] = value

    def __get__(self, obj, type=None):
        if obj is None:
            return self
        value = obj.__dict__.get(self.__name__, self._missing)
        if value is self._missing:
            value = self.func(obj)
            obj.__dict__[self.__name__] = value
        return value


class classproperty(object):
    '''
    类属性
    '''

    def __init__(self, getter):
        self.getter = getter

    def __get__(self, instance, owner):
        return self.getter(owner)


class Unique(object):
    '''
    单例基础类
    '''
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(Unique, cls).__new__(cls, *args, **kwargs)
            cls._instance.initialize(*args, **kwargs)
        return cls._instance

    def initialize(self, *args, **kwargs):
        '''
        这里初始化
        '''
        pass
