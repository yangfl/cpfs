#!/usr/bin/env python3
from __future__ import absolute_import

from cpfs.metadata import TmpMetadataConnection, METADATA_STORAGE_NAME
from cpfs.mkfs import init_metadata_db
from cpfs.storage import parser_add_url, init_storage_operations
import argparse
import os
import zlib


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser_add_url(parser)

    args = parser.parse_args()

    storage_operations = init_storage_operations(args.url[0])
    metadata_conn = TmpMetadataConnection(
        storage_operations.read(METADATA_STORAGE_NAME)
    )

    os.system('sqlitebrowser ' + metadata_conn.tmpfile_path)

    with open(metadata_conn.tmpfile_path, 'rb') as tmpfile_file:
        metadata_dump = zlib.compress(tmpfile_file.read())
    metadata_conn.close()
    storage_operations.write(METADATA_STORAGE_NAME, 0, metadata_dump)
    storage_operations.truncate(METADATA_STORAGE_NAME, len(metadata_dump))
    storage_operations.destory()
