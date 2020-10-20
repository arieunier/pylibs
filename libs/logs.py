import os, logging
from libs import config 

LOG_LEVEL = config.LOG_LEVEL

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
