from __future__ import absolute_import

import os
import json
from time import time
from threading import Lock, Condition, Event, Thread
from collections import defaultdict
from cpfs.compatibility import BytesIO, urlencode, Request, urlopen, HTTPError
from cpfs.metadata import METADATA_STORAGE_NAME
from cpfs.orderedset import OrderedSet
from cpfs.fragment import FragmentCache
from cpfs.logger import logger


def encode_multipart(params_dict):
    '''
    Build a multipart/form-data body with generated random boundary.
    '''
    boundary = '----%s' % hex(int(time() * 1000))
    data = []
    for key, value in params_dict.items():
        data.append('--{}'.format(boundary))
        if isinstance(value, str):
            data.append(
                'Content-Disposition: form-data; name="{}"\r\n'.format(key))
            data.append(value)
        else:
            data.append(
                'Content-Disposition: form-data; name="{}"; '
                'filename="hidden"'.format(key))
            data.append('Content-Type: application/octet-stream\r\n')
            data.append(value.decode('ISO-8859-1'))
    data.append('--{}--\r\n'.format(boundary))
    return '\r\n'.join(data), boundary


class StorageOperations:
    def __init__(self, hostname, path, username, password, additional_options):
        if not hostname:
            raise ValueError("access_token missing")
        self.access_token = hostname
        self.app_path = path
        self.quota = (0, (0, 0))
        self.destroyed = False
        self.dry_run = 'ro' in additional_options

        # blob control
        self.dict_files_buffer = {}
        self.set_new_files = set()
        # upload control
        self.mutex = Lock()
        self.all_jobs_done = Event()
        self.new_job = Event()
        self.queue_pending_files = OrderedSet()

        upload_thread = Thread(target=self._upload)
        upload_thread.start()

    def _get(self, base_url, parameters, headers=None):
        logger.debug(
            'bpan: get(base_url={}, parameters={}, headers={})'.format(
                base_url, parameters, headers))
        parameters['access_token'] = self.access_token
        req = Request('?'.join((base_url, urlencode(parameters))))
        if headers:
            req.headers.update(headers)
        try:
            return urlopen(req).read()
        except HTTPError as e:
            return e.read()

    def _path(self, name):
        return '/'.join((self.app_path, name))

    @staticmethod
    def _json(result):
        return json.loads(result.decode())

    def _post(self, base_url, parameters, data=b'', headers=None):
        logger.debug(
            'bpan: post(base_url={}, parameters={}, headers={})'.format(
                base_url, parameters, headers))
        if self.dry_run:
            logger.debug('bpan: dry_run')
            return b''
        parameters['access_token'] = self.access_token
        if data:
            data, boundary = encode_multipart(data)
            req = Request(
                '?'.join((base_url, urlencode(parameters))),
                data.encode('ISO-8859-1'))
            req.add_header(
                'Content-Type', 'multipart/form-data; boundary=%s' % boundary)
        else:
            req = Request(
                '?'.join((base_url, urlencode(parameters))), data)
        if headers:
            req.headers.update(headers)
        try:
            return urlopen(req).read()
        except HTTPError as e:
            return e.read()

    def _read_factory(self, name):
        path = self._path(name)

        def read_factory(offset, length):
            return self._get(
                'https://d.pcs.baidu.com/rest/2.0/pcs/file',
                {'method': 'download', 'path': path},
                headers={
                    'Range': 'bytes={}-{}'.format(offset, offset + length - 1)
                })

        return read_factory

    def _upload(self):
        last_wake = time()
        while True:
            if not self.queue_pending_files:
                while time() < last_wake + 600 and not self.destroyed:
                    self.new_job.wait()
                last_wake = time()
                self.new_job.clear()
            while self.queue_pending_files:
                with self.mutex:
                    name = self.queue_pending_files.pop(False)
                    self.dict_files_buffer[name].mutex.acquire()
                logger.debug(self._post(
                    'https://c.pcs.baidu.com/rest/2.0/pcs/file',
                    {
                        'method': 'upload', 'path': self._path(name),
                        'ondup': 'overwrite'},
                    {'file': self.dict_files_buffer[name].read(
                        0, self.size(name))}))
                self.dict_files_buffer[name].dirty = False
                self.set_new_files.discard(name)
                self.dict_files_buffer[name].mutex.release()
            if self.destroyed:
                break
        self.all_jobs_done.set()

    def close(self, name):
        if self.dict_files_buffer[name].dirty:
            if len(self.dict_files_buffer[name]):
                with self.mutex:
                    self.queue_pending_files.discard(name)
                    self.queue_pending_files.add(name)
                self.new_job.set()
            else:
                self.remove(name)

    def create(self, name):
        self.set_new_files.add(name)

    def destory(self):
        self.destroyed = True
        self.new_job.set()
        self.all_jobs_done.wait()

    def flush(self, name):
        pass

    def open(self, name, attr=None):
        with self.mutex:
            if name in self.queue_pending_files:
                self.queue_pending_files.discard(name)
        if name not in self.dict_files_buffer:
            self.dict_files_buffer[name] = FragmentCache(
                self._read_factory(name))
            if name in self.set_new_files:
                self.dict_files_buffer[name].dirty = True
                self.dict_files_buffer[name].length = 0
            elif attr:
                self.dict_files_buffer[name].length = attr.st_size
            else:
                self.dict_files_buffer[name].length = self._json(self._get(
                    'https://pcs.baidu.com/rest/2.0/pcs/file',
                    {'method': 'meta', 'path': self._path(name)}
                ))['list'][0]['size']

    def read(self, name, offset, length):
        return self.dict_files_buffer[name].read(offset, length)

    def remove(self, name):
        with self.dict_files_buffer[name].mutex:
            with self.mutex:
                if name in self.queue_pending_files:
                    self.queue_pending_files.discard(name)
            if name in self.set_new_files:
                self.set_new_files.discard(name)
            else:
                self._post(
                    'https://pcs.baidu.com/rest/2.0/pcs/file',
                    {'method': 'delete', 'path': self._path(name)})
            del self.dict_files_buffer[name]

    def statfs(self):
        if time() > self.quota[0] + 600:
            result = self._json(self._get(
                'https://pcs.baidu.com/rest/2.0/pcs/quota',
                {'method': 'info'}))
            self.quota = (time(), (result['used'], result['quota']))
        return self.quota[1]

    def size(self, name):
        return len(self.dict_files_buffer[name])

    def truncate(self, name, length):
        return self.dict_files_buffer[name].truncate(length)

    def write(self, name, offset, buf):
        return self.dict_files_buffer[name].write(offset, buf)
