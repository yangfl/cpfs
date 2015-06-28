'''
Register
Smart dict that auto generates unique id
'''
from threading import Lock, Condition
from random import randint
from time import time


class Full(Exception):
    def __str__(self):
        return 'No available id'


class Register(dict):
    def __init__(self, lower_bound=0, upper_bound=10):
        assert lower_bound < upper_bound
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        self.mutex = Lock()
        self.not_full = Condition(self.mutex)
        self.maxsize = self.upper_bound - self.lower_bound + 1
        self.last_id = lower_bound

    def _put(self, value, id_=None):
        if id_ is None:
            id_ = self.last_id
            while True:
                if id_ > self.upper_bound:
                    id_ = self.lower_bound
                if id_ not in self:
                    break
                id_ += 1
            self.last_id = id_ + 1
            '''
            while True:
                id_ = randint(self.start, self.stop)
                if id_ not in self:
                    break
            '''
        self[id_] = value
        return id_

    def __delitem__(self, id_):
        with self.mutex:
            dict.__delitem__(self, id_)
            self.not_full.notify()

    def register(self, value, block=True, timeout=0, id_=None):
        if id_ is not None:
            if type(id_) != int:
                raise TypeError("'id_' must be int")
            if not self.start <= id_ <= self.stop:
                raise ValueError("'id_' out of range")
        if block:
            with self.not_full:
                while True:
                    if len(self) < self.maxsize and id_ not in self:
                        return self._put(value, id_)
                    self.not_full.wait()
        elif timeout < 0:
            raise ValueError("'timeout' must be a non-negative number")
        else:
            with self.not_full:
                while True:
                    endtime = time() + timeout
                    if len(self) == self.maxsize or id_ in self:
                        remaining = endtime - time()
                        if remaining <= 0.0:
                            if len(self) == self.maxsize:
                                raise Full
                            if id_ in self:
                                raise KeyError("'id_' exist")
                    else:
                        return self._put(value, id_)
                    self.not_full.wait(remaining)

    remove = __delitem__
