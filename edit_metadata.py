#!/usr/bin/env python3
from __future__ import absolute_import

import argparse
import os
from cpfs.metadata import TmpMetadataConnection, read_metadata, write_metadata
from cpfs.storage import parser_add_url, init_storage_operations


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser_add_url(parser)

    args = parser.parse_args()

    storage_op = init_storage_operations(args.url[0])
    metadata_conn = TmpMetadataConnection(read_metadata(storage_op))

    os.system('sqlitebrowser ' + metadata_conn.tmpfile_path)

    write_metadata(storage_op, metadata_conn.dump())
    metadata_conn.close()
    storage_op.destory()
