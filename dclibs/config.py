import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv('salesforce.env', raise_error_if_not_found=True))
print(os.environ)

LOG_LEVEL = os.getenv('LOG_LEVEL','ERROR')
SANDBOX=False
SANDBOX_str=os.getenv('SANDBOX','True')
if (SANDBOX_str == 'True'):
    SANDBOX=True
else:
    SANDBOX=False

# CONNECTED APP AND JWT PART
CONSUMER_KEY=os.environ.get('CONSUMER_KEY','')
CONSUMER_SECRET=os.environ.get('CONSUMER_SECRET','')
USERNAME=os.environ.get('USERNAME','')
PASSWORD=os.environ.get('PASSWORD','')
SALESFORCEUSERID=os.environ.get('SALESFORCEUSERID','')
JWTKEY=os.environ.get('JWTKEY','')
JWTTOKEN_TTL=int(os.environ.get('JWTTOKEN_TTL','300')) #5min ttl

# PLATFORM EVENTS TO LISTEN TO
TOPICS=os.environ.get('TOPICS','')

# RABBIT MQ PART
CLOUDAMQP_URL = os.environ.get('CLOUDAMQP_URL', '')
EXCHANGE = 'rabbitexchange'

# KAFKA PART
KAFKA_URL=  os.getenv('KAFKA_URL','')
KAFKA_CLIENT_CERT=  os.getenv('KAFKA_CLIENT_CERT','')
KAFKA_CLIENT_CERT_KEY=  os.getenv('KAFKA_CLIENT_CERT_KEY','')
KAFKA_TRUSTED_CERT=  os.getenv('KAFKA_TRUSTED_CERT','')
KAFKA_PREFIX=os.getenv('KAFKA_PREFIX','')
if (KAFKA_PREFIX != ''):
    KAFKA_PREFIX = KAFKA_PREFIX + '.' 
KAFKA_GROUP_ID=os.getenv('KAFKA_CONSUMERGRP', KAFKA_PREFIX  + 'my-consumer-group')
KAFKA_USE_GROUP=os.getenv('KAFKA_USE_GROUP', 'True')
KAFKA_TOPIC_WRITE=os.getenv('KAFKA_TOPIC_WRITE', 'mytopicwrite')
# REDIS
REDIS_URL = os.getenv('REDIS_URL','')
USE_TLS = os.getenv('USE_REDIS_TLS','FALSE')
REDIS_TLS_URL = os.getenv('REDIS_TLS_URL','')
REDIS_JWTTOKEN='jwt_token'

#DEFAULT LISTENING TOPIC
SUBSCRIBE_CHANNEL=os.getenv('SUBSCRIBE_CHANNEL', "")

#NOTIFICATION SERVICE
SERVICE_REGISTRATION=os.getenv('SERVICE_REGISTRATION','registrationservice')
SERVICE_NOTIFICATION=os.getenv('SERVICE_NOTIFICATION', "notifications")
SERVICE_ACCOUNT_CREATION=os.getenv('SERVICE_ACCOUNT_CREATION', "accountscreation")
SERVICE_BULK=os.getenv('SERVICE_BULK', "bulkservice")

DATABASE_URL = os.getenv('DATABASE_URL','')
SALESFORCE_SCHEMA = os.getenv("SALESFORCE_SCHEMA", "salesforce")

BUCKETEER_AWS_ACCESS_KEY_ID=  os.getenv('BUCKETEER_AWS_ACCESS_KEY_ID','')
BUCKETEER_AWS_REGION= os.getenv('BUCKETEER_AWS_REGION','')
BUCKETEER_AWS_SECRET_ACCESS_KEY= os.getenv("BUCKETEER_AWS_SECRET_ACCESS_KEY", "")
BUCKETEER_BUCKET_NAME= os.getenv("BUCKETEER_BUCKET_NAME", "") 

QUEUING_SYSTEM=os.environ.get('QUEUING_SYSTEM','KAFKA').upper()
QUEUING_CLOUDAMQP='CLOUDAMQP'
QUEUING_KAFKA='KAFKA'

OPPYCOPY_DEFAULT_SEND_METHOD=os.environ.get('OPPYCOPY_DEFAULT_SEND_METHOD','BULK').upper()

PREFIX_ACCOUNT='001'
PREFIX_CONTACT='003'