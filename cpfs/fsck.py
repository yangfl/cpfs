from __future__ import absolute_import

from itertools import chain
from .compatibility import reduce
from .logger import logger
from stat import S_IFREG, S_IFLNK, S_IFDIR


FSCK_RULES = dict((
    # (name,
    #  (SQL, output formatter, fix SQL format, fix SQL parameters formatter)),
    ('nlink', (
        """
        SELECT inodes.inode, inodes.nlink,
            COUNT(contents.parent_inode) AS real_nlink
        FROM inodes
        LEFT JOIN contents
        ON inodes.inode = contents.inode
        GROUP BY inodes.inode
        HAVING inodes.nlink != real_nlink
        """,
        lambda name, entry: "{}: inode '{}' nlink '{}' -> '{}'".format(
            name, *entry
        ),
        "UPDATE inodes SET nlink = ? WHERE inode = ?",
        lambda entry: (entry[2], entry[0]),
    )),
    ('invalid_symlink', (
        """
        SELECT inode
        FROM (
            SELECT inodes.inode AS inode, targets.path AS path
            FROM inodes
            LEFT OUTER JOIN targets
            ON inodes.inode = targets.inode
            WHERE inodes.mode & {0} == {0})
        WHERE path IS NULL
        """.format(S_IFLNK),
        lambda name, entry: "{}: symlink '{}' target 'null'".format(
            name, entry[0]
        ),
        "INSERT INTO targets (inode, path) VALUES (?, x'{}')".format(
            ''.join(hex(ord(c))[2:] for c in 'invaild')
        ),
        lambda entry: entry,
    )),
    ('invalid_crc32', (
        """
        SELECT inode, crc32
        FROM inodes
        WHERE mode & 0xE000 | {0} != {0} AND crc32
        """.format(S_IFREG),
        lambda name, entry:
            "{}: inode '{}' not regular file but crc32 '{}'".format(
                name, *entry
        ),
        "UPDATE inodes SET crc32 = NULL WHERE inode = ?",
        lambda entry: (entry[0],),
    )),
    ('invalid_dir_nlink', (
        """
        SELECT inode, nlink
        FROM inodes
        WHERE mode & 0xE000 | {0} == {0} AND nlink > 1
        """.format(S_IFDIR),
        lambda name, entry: "{}: inode '{}' dir but nlink '{}'".format(
            name, *entry
        ),
        "",
        (),
    )),
))

CONVENTIONAL_CHECKS = (
    'nlink', 'invalid_symlink', 'invalid_crc32', 'invalid_dir_nlink'
)


def do_fsck(name):
    fsck_rule = FSCK_RULES[name]

    def fsck_func(conn, verbose=False, test=False):
        cur = conn.cursor()
        list_invalid_entries = cur.execute(fsck_rule[0]).fetchall()
        if list_invalid_entries:
            if verbose and fsck_rule[1]:
                for invalid_entry in list_invalid_entries:
                    logger.debug(fsck_rule[1](name, invalid_entry))
            if not test and fsck_rule[2]:
                cur.executemany(fsck_rule[2],
                                map(fsck_rule[3], list_invalid_entries))
                conn.commit()
        return len(list_invalid_entries), test and fsck_rule[2]

    return fsck_func


def do_fsck_and_return(name, conn, verbose, test):
    error, test_only = do_fsck(name)(conn, verbose, test)
    if error:
        logger.warning('{} error: {}'.format(
            name.capitalize().replace('_', ' '), error
        ))
    return error and (test_only and 4 or 1)


def do_fscks(list_names, conn, verbose, test):
    return reduce(
        lambda prev_exit_code, name:
            max(prev_exit_code, do_fsck_and_return(name, conn, verbose, test)),
        chain((0,), list_names)
    )
