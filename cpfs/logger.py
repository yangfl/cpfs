from __future__ import absolute_import

import logging
from . import __name__

logger = logging.getLogger(__name__)


def set_logger(verbose=False, full=False):
    if verbose:
        logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    if full:
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s: %(message)s')
        ch.setFormatter(formatter)
        logging.getLogger(__name__).addHandler(ch)
    logger.addHandler(ch)
