from __future__ import absolute_import

import imp
import os
from .compatibility import urlparse


def parser_add_url(parser):
    parser.add_argument(
        'url', metavar='scheme://[[[username[:password]@]hostname][/path]]',
        nargs=1
    )


def init_storage_operations(url, mount_arguments=''):
    parsed_url = urlparse(url)
    return imp.load_source(
        'remote',
        os.path.join(
            os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
            'remote', parsed_url.scheme + '.py'
        )
    ).StorageOperations(
        parsed_url.hostname, parsed_url.path,
        parsed_url.username, parsed_url.password,
        dict(
            '=' in argument and argument.split('=', 1) or (argument, 1)
            for argument in mount_arguments.split(',')
        )
    )
