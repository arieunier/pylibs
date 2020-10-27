from kafka import KafkaProducer,KafkaConsumer, KafkaClient
from heroku_kafka import HerokuKafkaProducer, HerokuKafkaConsumer
from kafka import  TopicPartition
from kafka.structs import OffsetAndMetadata
import os 
import uuid, ujson
from kafka.errors import KafkaError
#Consumer, KafkaError, Producer
import ujson 
import datetime
from libs import logs, kafka_utils, config

LOGGER = logs.LOGGER

KAFKA_URL=  config.KAFKA_URL
KAFKA_CLIENT_CERT=  config.KAFKA_CLIENT_CERT
file = open('static/kafka_client_cert', "w")
data = file.write(KAFKA_CLIENT_CERT)
file.close()


KAFKA_CLIENT_CERT_KEY=  config.KAFKA_CLIENT_CERT_KEY
file = open('static/kafka_client_key', "w")
data = file.write(KAFKA_CLIENT_CERT_KEY)
file.close()


KAFKA_TRUSTED_CERT=  config.KAFKA_TRUSTED_CERT
file = open('static/kafka_ca', "w")
data = file.write(KAFKA_TRUSTED_CERT)
file.close()

KAFKA_PREFIX=  config.KAFKA_PREFIX
KAFKA_GROUP_ID=config.KAFKA_GROUP_ID

LOGGER.debug("KAFKA_PREFIX="+KAFKA_PREFIX)
LOGGER.debug("KAFKA_GROUP_ID="+KAFKA_GROUP_ID)

"""
    All the variable names here match the heroku env variable names.
    Just pass the env values straight in and it will work.
"""
producer = None

def init():
    global producer

    producer = KafkaProducer(
            bootstrap_servers =KAFKA_URL.replace('kafka+ssl://','').split(','),
            security_protocol ='SSL',
            ssl_check_hostname=False,
            ssl_cafile ='static/kafka_ca',
            ssl_certfile ='static/kafka_client_cert',
            ssl_keyfile= 'static/kafka_client_key'
            )

def createTopic(topicName):
    from kafka import KafkaClient
    client = KafkaClient(bootstrap_servers =KAFKA_URL.replace('kafka+ssl://','').split(','),
            security_protocol ='SSL',
            ssl_check_hostname=False,
            ssl_cafile ='static/kafka_ca',
            ssl_certfile ='static/kafka_client_cert',
            ssl_keyfile= 'static/kafka_client_key')
    client.add_topic(topicName)
    #topic_list = []
    #topic_list.append(NewTopic(name="example_topic", num_partitions=1, replication_factor=1))
    #admin_client.create_topics(new_topics=topic_list, validate_only=False)

def sendToKafka(data, topic=None):
    global producer
    """
    The .send method will automatically prefix your topic with the KAFKA_PREFIX
    NOTE: If the message doesn't seem to be sending try `producer.flush()` to force send.
    """

    if topic==None:
        sndTopic=KAFKA_PREFIX +  KAFKA_TOPIC_WRITE
    else:
        sndTopic=KAFKA_PREFIX +  topic

    LOGGER.debug("about to send {} to topic {}".format(data, sndTopic))
    producer.send(sndTopic, ujson.dumps(data).encode("UTF-8"))
    producer.flush()
    LOGGER.debug("done")




def receiveFromKafka(mode, topic_override, callback):
    TOPIC = topic_override

    LOGGER.info("Will use topic = {}".format(TOPIC))
    """
    consumer = KafkaConsumer(
        #KAKFA_TOPIC, # Optional: You don't need to pass any topic at all
        bootstrap_servers= KAFKA_URL, # Url string provided by heroku
        ssl_certfile= KAFKA_CLIENT_CERT, # Client cert string
        ssl_keyfile= KAFKA_CLIENT_CERT_KEY, # Client cert key string
        ssl_cafile= KAFKA_TRUSTED_CERT, # Client trusted cert string
        auto_offset_reset="latest",
        max_poll_records=100,
        enable_auto_commit=True,
        auto_commit_interval_ms=100,
        api_version = (0,9),
        #max_poll_interval_ms=120000,
        session_timeout_ms=180000,
        request_timeout_ms=185000,
        heartbeat_interval_ms=3000
        
    )
    """
    
    consumer = HerokuKafkaConsumer(
        #KAKFA_TOPIC, # Optional: You don't need to pass any topic at all
        url= KAFKA_URL, # Url string provided by heroku
        ssl_cert= KAFKA_CLIENT_CERT, # Client cert string
        ssl_key= KAFKA_CLIENT_CERT_KEY, # Client cert key string
        ssl_ca= KAFKA_TRUSTED_CERT, # Client trusted cert string
        prefix= KAFKA_PREFIX, # Prefix provided by heroku,
        auto_offset_reset="smallest",
        max_poll_records=100,
        enable_auto_commit=True,
        auto_commit_interval_ms=100,
        group_id=KAFKA_GROUP_ID,
        api_version = (0,9),
        session_timeout_ms=180000,
        request_timeout_ms=185000,
        heartbeat_interval_ms=3000
    )


    """
    To subscribe to topic(s) after creating a consumer pass in a list of topics without the
    KAFKA_PREFIX.
    """
    partition=1
    
    tp = TopicPartition(KAFKA_PREFIX + TOPIC, partition)
    if (mode == "subscribe"):
        consumer.subscribe(topics=(TOPIC))
    elif (mode == "assign"):
        consumer.assign([tp])

    # display list of partition assignerd
    assignments = consumer.assignment()
    for assignment in assignments:
        LOGGER.info(assignment)
    
    partitions=consumer.partitions_for_topic(KAFKA_PREFIX + TOPIC)
    if (partitions):
        for partition in partitions:
            LOGGER.info("Partition="+str(partition))
    
    
    topics=consumer.topics()
    if (topics):
        for topic in topics:
            LOGGER.info("Topic:"+topic)
    #exit(1)
    LOGGER.info('waiting ..')
    """
    .assign requires a full topic name with prefix
    """
    

    """
    Listening to events it is exactly the same as in kafka_python.
    Read the documention linked below for more info!
    """
    i=0
    for message in consumer:
        try:
            LOGGER.info ("%i %s:%d:%d: key=%s value=%s" % (i, message.topic, message.partition,
                                              message.offset, message.key,
                                              message.value))

            dictValue = ujson.loads(message.value)
            LOGGER.info(dictValue)

            callback(dictValue)
            
            consumer.commit()
        except Exception as e :
            import traceback
            traceback.print_exc()
            consumer.commit()

        i += 1

