'''
MultithreadConnection
Share sqlite3 connect between multi thread
'''
import os
import sqlite3
import tempfile
import zlib
from threading import Lock, Condition

METADATA_STORAGE_NAME = '0'


class WriteableCursor(sqlite3.Cursor):
    def __init__(self, conn):
        super(WriteableCursor, self).__init__(conn)
        self.conn = conn

    def __enter__(self):
        self.conn.waiting_list.append(None)
        self.conn.mutex.acquire()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.conn.mutex.release()
        self.conn.waiting_list.pop()
        if not self.conn.waiting_list:
            try:
                self.conn.empty.notify()
            except RuntimeError:
                pass

    def acquire(self):
        return self.__enter__()

    def release(self):
        return self.__exit__(None, None, None)


class MultithreadConnection(sqlite3.Connection):
    def __init__(self, *args, **kwargs):
        super(MultithreadConnection, self).__init__(
            check_same_thread=False, *args, **kwargs)
        self.waiting_list = []
        self.mutex = Lock()
        self.empty = Condition(self.mutex)

    def _write_execute(self, method, sql, parameters=()):
        self.waiting_list.append(None)
        with self.mutex:
            cur = method(sql, parameters)
        self.waiting_list.pop()
        if not self.waiting_list:
            try:
                self.empty.notify()
            except RuntimeError:
                pass
        return cur

    def commit(self):
        with self.empty:
            while self.waiting_list:
                self.empty.wait()
            super(MultithreadConnection, self).commit()

    def write_execute(self, sql, parameters=()):
        return self._write_execute(self.execute, sql, parameters)

    def write_executemany(self, sql, parameters=()):
        return self._write_execute(self.executemany, sql, parameters)

    def writeable_cursor(self):
        return WriteableCursor(self)


def connect(*args, **kwargs):
    return MultithreadConnection(*args, **kwargs)


class TmpMetadataConnection(MultithreadConnection):
    def __init__(self, compressed_data=None):
        fd, self.tmpfile_path = tempfile.mkstemp()
        try:
            with os.fdopen(fd, 'wb') as tmpfile_fh:
                if compressed_data:
                    tmpfile_fh.write(zlib.decompress(compressed_data))
        except:
            os.remove(self.tmpfile_path)
            raise
        super(TmpMetadataConnection, self).__init__(self.tmpfile_path)

    def dump(self):
        self.commit()
        with open(self.tmpfile_path, 'rb') as tmpfile_file:
            return zlib.compress(tmpfile_file.read())

    def close(self):
        super(TmpMetadataConnection, self).close()
        os.remove(self.tmpfile_path)


def read_metadata(storage_op):
    storage_op.open(METADATA_STORAGE_NAME)
    dump = storage_op.read(METADATA_STORAGE_NAME, 0, -1)
    storage_op.close(METADATA_STORAGE_NAME)
    return dump


def write_metadata(storage_op, dump):
    storage_op.open(METADATA_STORAGE_NAME)
    storage_op.write(METADATA_STORAGE_NAME, 0, dump)
    storage_op.truncate(METADATA_STORAGE_NAME, len(dump))
    storage_op.flush(METADATA_STORAGE_NAME)
    storage_op.close(METADATA_STORAGE_NAME)
