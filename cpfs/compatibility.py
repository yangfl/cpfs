__all__ = (
    'PY2', 'blob_type', 'map', 'reduce', 'Queue', 'input', 'urlparse',
    'BytesIO',
)

if bytes == str:
    PY2 = True
    blob_type = buffer
    from itertools import imap as map
    reduce = reduce
    from Queue import Queue
    input = raw_input
    from urlparse import urlparse
    from cStringIO import StringIO as BytesIO
else:
    PY2 = False
    blob_type = bytes
    map = map
    from functools import reduce
    from queue import Queue
    input = input
    from urllib.parse import urlparse
    from io import BytesIO
