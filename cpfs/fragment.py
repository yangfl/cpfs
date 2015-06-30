'''
FragmentCache
'''
from __future__ import absolute_import

import bisect
from threading import RLock
from .compatibility import BytesIO


class FragmentCache:
    def __init__(self, factory):
        if not callable(factory):
            raise TypeError('first argument must be callable')
        self.factory = factory
        self.stream = BytesIO()
        self.cached_scope = [-1]
        # odd: fragment beginning
        # even: fragment ending
        self.dirty = False
        self.length = 0
        self.mutex = RLock()

    def __len__(self):
        return max(0, self.cached_scope[-1], self.length)

    def load(self, start_offset, end_offset, empty=False):
        with self.mutex:
            lower_index_plus_one = bisect.bisect_right(
                self.cached_scope, start_offset)
            upper_index = bisect.bisect_left(self.cached_scope, end_offset)
            if not lower_index_plus_one & 1 and \
                    lower_index_plus_one == upper_index:
                return
            if not empty:
                scope_slice_index = zip(
                    [start_offset] +
                    self.cached_scope[lower_index_plus_one:upper_index] +
                    [end_offset],
                    range(lower_index_plus_one - 1, upper_index + 1))
                current_slice_index = next(scope_slice_index)
                try:
                    while True:
                        last_slice_index, current_slice_index = \
                            current_slice_index, next(scope_slice_index)
                        if current_slice_index[1] & 1:
                            self.stream.seek(last_slice_index[0])
                            self.stream.write(self.factory(
                                self, last_slice_index[0],
                                current_slice_index[0] - last_slice_index[0]))
                except StopIteration:
                    pass
            del self.cached_scope[lower_index_plus_one:upper_index]
            if lower_index_plus_one & 1:
                self.cached_scope.insert(lower_index_plus_one, start_offset)
            if upper_index & 1:
                self.cached_scope.insert(
                    lower_index_plus_one + 1, end_offset)
            for check_index in (
                    lower_index_plus_one, lower_index_plus_one + 2):
                while check_index < len(self.cached_scope) and \
                        self.cached_scope[check_index] == \
                        self.cached_scope[check_index - 1]:
                    del self.cached_scope[check_index], \
                        self.cached_scope[check_index - 1]

    def read(self, offset, length):
        with self.mutex:
            self.load(offset, offset + length)
            self.stream.seek(offset)
            return self.stream.read(length)

    def truncate(self, length):
        with self.mutex:
            self.stream.truncate(length)
            index = bisect.bisect_left(self.cached_scope, length)
            del self.cached_scope[index:]
            if not index & 1:
                self.cached_scope.append(length)
            self.dirty = True
            self.length = length
        return length

    def write(self, offset, buf):
        with self.mutex:
            self.load(offset, offset + len(buf), True)
            self.stream.seek(offset)
            self.stream.write(buf)
            self.dirty = True
        return len(buf)
