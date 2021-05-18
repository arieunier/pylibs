import redis 
import os
import ujson
import json
from datetime import datetime 
from urllib.parse import urlparse

from dclibs import logs, config

import traceback
REDIS_URL = config.REDIS_URL
REDIS_TLS_URL = config.REDIS_TLS_URL
USE_TLS = config.USE_TLS
REDIS_CONN = None 

LOGGER = logs.LOGGER


def __connect():
    global REDIS_CONN
    global REDIS_URL
    if (REDIS_URL != ''):
        LOGGER.info("Redis Url->{}".format(REDIS_URL))
        LOGGER.info("Redis TLS Url->{}".format(REDIS_TLS_URL))
        LOGGER.info("Redis USE_TLS ->{}".format(USE_TLS))
        if (USE_TLS == "TRUE"):
            # TLS VERSION
            url = urlparse(REDIS_URL)
            REDIS_CONN = redis.Redis(host=url.hostname, port=url.port, username=url.username, password=url.password, ssl=True, ssl_cert_reqs=None)
        else:
            # NORMAL VERSION
            url = urlparse(REDIS_URL)
            REDIS_CONN = redis.from_url(REDIS_URL)

        REDIS_CONN.set('key','value')
        REDIS_CONN.expire('key', 300) # 5 minutes
        LOGGER.info("{}  - Initialization done Redis" .format(datetime.now()))

def __display_RedisContent():
    try:
        global REDIS_CONN
        if (REDIS_CONN == None):
            __connect()
        if (REDIS_CONN != None):
            for keys in REDIS_CONN.scan_iter():
                LOGGER.info(keys)
        cacheData = __getCache('keys *')
        if cacheData != None:
            for entry in cacheData:
                LOGGER.info(entry)
    except Exception as e:
        REDIS_CONN = None
        traceback.print_exc()

def __setCache(key, data, ttl):
    try:
        global REDIS_CONN
        if (REDIS_CONN == None):
            __connect()
        key_str = ujson.dumps(key)
        LOGGER.debug('Key->{}'.format(key))
        LOGGER.debug('Data->{}'.format(data))
        if (REDIS_CONN != None):
            LOGGER.debug('Storing in Redis')    
            REDIS_CONN.set(key_str, data)
            REDIS_CONN.expire(key_str, ttl)
            
    except Exception as e:
        REDIS_CONN = None
        traceback.print_exc()

def __getCache(key):
    try:
        global REDIS_CONN
        if (REDIS_CONN == None):
            __connect()
        
        key_str = ujson.dumps(key)
        if (REDIS_CONN != None):
            LOGGER.debug('Reading in Redis')
            return REDIS_CONN.get(key_str)
        return None 
    except Exception as e:
        REDIS_CONN = None
        traceback.print_exc()
        return None

def __delCache(key):
    try:
        global REDIS_CONN
        if (REDIS_CONN == None):
            __connect()

        key_str = ujson.dumps(key)
        if (REDIS_CONN != None):
            LOGGER.debug('Deleting in Redis')
            REDIS_CONN.delete(key_str)
    except Exception as e:
        REDIS_CONN = None
        traceback.print_exc()
    
