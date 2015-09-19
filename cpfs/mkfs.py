from __future__ import absolute_import

from .compatibility import blob_type
from stat import *
from time import time
from collections import OrderedDict
from itertools import chain

try:
    from llfuse import ROOT_INODE
except ImportError:
    ROOT_INODE = 1


def sql_create_db(db_structure):
    return ';\n'.join(chain(
        # PRAGMA
        (
            'PRAGMA {} = {}'.format(*pragma_definition)
            for pragma_definition in db_structure[0]
        ),
        # TABLE
        (
            'CREATE TABLE {} (\n{}\n)'.format(
                table_structure[0],
                ',\n'.join(chain(
                    (
                        ' '.join(field_structure)
                        for field_structure in table_structure[1].items()
                    ),
                    (
                        'UNIQUE ({}, {})'.format(*unique_structure)
                        for unique_structure in table_structure[2]
                    ),
                    (
                        'FOREIGN KEY ({}) REFERENCES {}({})'.format(
                            *foreign_key_structure
                        ) for foreign_key_structure in table_structure[3]
                    ),
                ))
            )
            for table_structure in db_structure[1]
        ),
        # INDEX
        (
            'CREATE INDEX {} ON {} ({})'.format(*index_structure)
            for index_structure in db_structure[2]
        ),
    ))


METADATA_DB_PRAGMA = (
    ('foreign_keys', 'true'),
)
TABLE_INODES_STRUCTURE = OrderedDict((
    ('inode', 'INTEGER PRIMARY KEY'),
    ('generation', 'INT NOT NULL DEFAULT 0'),
    ('mode', 'SMALLINT NOT NULL'),
    ('nlink', 'INT NOT NULL DEFAULT 0'),
    ('uid', 'INT NOT NULL'),
    ('gid', 'INT NOT NULL'),
    ('rdev', 'INT NOT NULL DEFAULT 0'),
    ('size', 'INT NOT NULL DEFAULT 0'),
    ('atime', 'REAL NOT NULL'),
    ('ctime', 'REAL NOT NULL'),
    ('mtime', 'REAL NOT NULL'),
))
TABLE_CONTENTS_STRUCTURE = OrderedDict((
    ('rowid', 'INTEGER PRIMARY KEY'),
    ('name', "BLOB(256) NOT NULL CHECK (TYPEOF(name) == 'blob')"),
    ('inode', 'INT NOT NULL REFERENCES inodes(inode)'),
    ('parent_inode', 'INT NOT NULL REFERENCES inodes(inode)'),
))
TABLE_CONTENTS_UNIQUE = (
    ('name', 'parent_inode'),
)
TABLE_TARGETS_STRUCTURE = OrderedDict((
    ('inode', 'INTEGER PRIMARY KEY'),
    ('path', "BLOB NOT NULL CHECK (TYPEOF(path) == 'blob')"),
))
TABLE_TARGETS_FOREIGN_KEY = (
    ('inode', 'inodes', 'inode'),
)
TABLE_XATTRS_STRUCTURE = OrderedDict((
    ('rowid', 'INTEGER PRIMARY KEY'),
    ('inode', 'INT NOT NULL REFERENCES inodes(inode)'),
    ('key', "BLOB NOT NULL CHECK (TYPEOF(key) == 'blob')"),
    ('value', "BLOB CHECK (TYPEOF(value) == 'blob')"),
))
TABLE_XATTRS_UNIQUE = (
    ('inode', 'key'),
)
METADATA_DB_STRUCTURE = (
    METADATA_DB_PRAGMA,
    (
        ('inodes', TABLE_INODES_STRUCTURE, (), ()),
        ('contents', TABLE_CONTENTS_STRUCTURE, TABLE_CONTENTS_UNIQUE, ()),
        ('targets', TABLE_TARGETS_STRUCTURE, (), TABLE_TARGETS_FOREIGN_KEY),
        ('xattrs', TABLE_XATTRS_STRUCTURE, TABLE_XATTRS_UNIQUE, ())),
    (
        # ('inode_index', 'contents', 'inode'),
    )
)
SQL_CREATE_METADATA_DB = sql_create_db(METADATA_DB_STRUCTURE)


def init_metadata_db(conn, uid=0, gid=0):
    cur = conn.cursor()
    cur.executescript(SQL_CREATE_METADATA_DB)
    # Insert root directory
    cur.execute(
        "INSERT INTO inodes (inode, mode, nlink, uid, gid, "
        "atime, ctime, mtime) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (ROOT_INODE, S_IFDIR | S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP |
         S_IXGRP | S_IROTH | S_IXOTH, 1, uid, gid) + (time(),) * 3)
    cur.execute(
        "INSERT INTO contents (name, parent_inode, inode) VALUES (?,?,?)",
        (blob_type(b'..'), ROOT_INODE, ROOT_INODE))
    conn.commit()
