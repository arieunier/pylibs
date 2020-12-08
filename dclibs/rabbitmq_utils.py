import functools
import logging
import pika
import threading
import time
import urllib.parse
import ujson
import sys

import random
import string
from dclibs import logs, rabbitmq_utils, config
import ujson



CLOUDAMQP_URL = config.CLOUDAMQP_URL
EXCHANGE = config.EXCHANGE

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logs.LOGGER
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

credentials=None
channel = None
parameters = None

url = urllib.parse.urlparse(CLOUDAMQP_URL)
connection = None 


def init():
    global credentials, channel, parameters, connection

    credentials = pika.PlainCredentials(url.username, url.password)
    # Note: sending a short heartbeat to prove that heartbeats are still
    # sent even though the worker simulates long-running work
    parameters =  pika.ConnectionParameters(host=url.hostname, virtual_host=url.path[1:], credentials=credentials, heartbeat=600)
    connection = pika.BlockingConnection(parameters)
    
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)

    

def createQueues(queueNames):
    #Correct config
    # channel.exchange_declare(exchange=EXCHANGE, exchange_type="direct", passive=False, durable=True, auto_delete=True, arguments={'x-message-ttl' : max})
    # channel.queue_declare(queue=CLOUDAMQP_QUEUE, auto_delete=True, durable=True)
    
    # For current workers ...
    global channel, EXCHANGE
    import sys
    max = sys.maxsize
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="direct", passive=False, durable=True, auto_delete=False, arguments={'x-message-ttl' : max})    
    
    for queue in queueNames:
        channel.queue_declare(queue=queue, auto_delete=False, durable=True)
        channel.queue_bind(queue=queue, exchange=EXCHANGE, routing_key=queue)

    channel.basic_qos(prefetch_count=1)

def receiveMessage(queueName, functionName):
    global channel, connection
    __checkQueue()
    # set up subscription on the queue

    channel.basic_consume( queueName,functionName, auto_ack=True)
    channel.start_consuming() # start consuming (blocks)
    
    connection.close()


def __checkQueue():
    global channel, connection
    if (channel == None or connection == None or channel.is_open == False):
        connection = None
        channel = None
        init()

def _retrySendData(data, queue):
    global channel
    try:
        # reinit
        __checkQueue()
        channel.basic_publish(exchange='',
                                     routing_key=queue,
                                         body=data,
                      properties=pika.BasicProperties(
                         delivery_mode = 2 # make message persistent
                      ))
        #logger.debug("[x] sent")
    except Exception as e:
        import traceback
        traceback.print_exc()
        LOGGER.error(e.__str__())
        LOGGER.error("{} - ERROR while trying to connect to RabbitMQ. Data are lost ! #FIXME #TODO - {}".format(datetime.now(), e.__str__()))

def sendToRabbit(data, queue):
    # send a message
    global channel
    try:
        __checkQueue()
        dataEncoded = ujson.dumps(data)
        channel.basic_publish(exchange='',
                             routing_key=queue,
                                         body=dataEncoded,
                      properties=pika.BasicProperties(
                         delivery_mode = 2 # make message persistent
                      ))
        LOGGER.debug("[x] sent")
    except pika.exceptions.ConnectionClosed as e:
        try:
            logger.error("Error with Rabbit MQ Connection. Trying to reinit it")
            init()
            channel.basic_publish(exchange='',
                                     routing_key=queue,
                                         body=dataEncoded,
                      properties=pika.BasicProperties(
                         delivery_mode = 2 # make message persistent
                      ))
        except Exception as e:
            init()
            _retrySendData(dataEncoded, queue)
    except Exception as e:
        init()
        _retrySendData(dataEncoded, queue)

