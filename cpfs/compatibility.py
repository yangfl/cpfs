if bytes == str:
    PY2 = True
    blob_type = buffer
    from itertools import imap as map
    reduce = reduce
    from Queue import Queue
    input = raw_input
    from urlparse import urlparse
    from cStringIO import StringIO as BytesIO
    from urllib import urlencode
    from urllib2 import Request
    from urllib2 import urlopen
    from urllib2 import HTTPError
else:
    PY2 = False
    blob_type = bytes
    map = map
    from functools import reduce
    from queue import Queue
    input = input
    from urllib.parse import urlparse
    from io import BytesIO
    from urllib.parse import urlencode
    from urllib.request import Request
    from urllib.request import urlopen
    from urllib.error import HTTPError
