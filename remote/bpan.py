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
    def __init__(self, hostname, path, username, password,
                 **additional_options):
        if not hostname:
            raise ValueError("access_token missing")
        self.access_token = hostname
        self.app_path = path
        self.quota = (0, (0, 0))
        self.destroyed = False

        # blob control
        self.dict_files_buffer = {}
        # upload control
        self.mutex = Lock()
        self.all_jobs_done = Event()
        self.new_job = Event()
        self.queue_pending_files = OrderedSet()

        upload_thread = Thread(target=self._upload)
        upload_thread.start()

    def _get(self, base_url, parameters, headers=None):
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
        parameters['access_token'] = self.access_token
        if data:
            coded_parameters, boundary = encode_multipart(data)
            req = Request(
                '?'.join((base_url, urlencode(parameters))),
                coded_parameters.encode('ISO-8859-1'))
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
        while True:
            if not self.queue_pending_files:
                self.new_job.wait()
                self.new_job.clear()
            while self.queue_pending_files:
                with self.mutex:
                    name = self.queue_pending_files.pop(False)
                logger.debug(self._post(
                    'https://c.pcs.baidu.com/rest/2.0/pcs/file',
                    {'method': 'upload', 'path': self._path(name)},
                    {'file': self.dict_files_buffer[name].read(
                        0, len(self.dict_files_buffer[name]))}))
            if self.destroyed:
                break
        self.all_jobs_done.set()

    def close(self, name):
        if self.dict_files_buffer[name].dirty:
            if not len(self.dict_files_buffer[name]):
                if self.dict_files_buffer[name].orig_exist:
                    self.remove(name)
            else:
                with self.mutex:
                    if name in self.queue_pending_files:
                        self.queue_pending_files.discard(name)
                    self.queue_pending_files.add(name)
                self.new_job.set()

    def destory(self):
        self.destroyed = True
        self.new_job.set()
        self.all_jobs_done.wait()

    def flush(self, name):
        pass

    def open(self, name):
        with self.mutex:
            if name in self.queue_pending_files:
                self.queue_pending_files.discard(name)
        file_metadata = self._get(
            'https://pcs.baidu.com/rest/2.0/pcs/file',
            {'method': 'meta', 'path': self._path(name)})
        file_cache = FragmentCache(self._read_factory(name))
        self.dict_files_buffer[name] = file_cache
        file_cache.orig_exist = b'"error_code":31066' not in file_metadata
        if name == METADATA_STORAGE_NAME:
            file_cache.orig_len = \
                file_cache.orig_exist and \
                self._json(file_metadata)['list'][0]['size'] or 0

    def read(self, name, offset, length):
        if length == -1:
            length = self.dict_files_buffer[name].orig_len - offset
            assert length >= 0
        return self.dict_files_buffer[name].read(offset, length)

    def remove(self, name):
        with self.mutex:
            if name in self.queue_pending_files:
                self.queue_pending_files.discard(name)
        if self.dict_files_buffer[name].orig_exist:
            self._post(
                'https://pcs.baidu.com/rest/2.0/pcs/file',
                {'method': 'delete', 'path': self._path(name)})
        if name in self.dict_files_buffer:
            del self.dict_files_buffer[name]

    def statfs(self):
        if time() > self.quota[0] + 600:
            result = self._json(self._get(
                'https://pcs.baidu.com/rest/2.0/pcs/quota',
                {'method': 'info'}))
            self.quota = (time(), (result['used'], result['quota']))
        return self.quota[1]

    def truncate(self, name, length):
        return self.dict_files_buffer[name].truncate(length)

    def write(self, name, offset, buf):
        return self.dict_files_buffer[name].write(offset, buf)
