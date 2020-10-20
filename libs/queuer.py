from libs import config, rabbitmq_utils, kafka_utils, logs

LOGGER=logs.LOGGER
TOPICS=config.TOPICS

def initQueuer():
    if (config.QUEUING_SYSTEM == config.QUEUING_CLOUDAMQP):
        LOGGER.info("Rabbit MQ Mode")
        rabbitqueues=[]
        for topic in TOPICS.split(";"):
            LOGGER.debug("Subscribing to {}".format(topic))
            rabbitqueues.append(topic[1:len(topic)].replace('/','_'))
        #rabbitqueues.append(config.SERVICE_NOTIFICATION)
        #rabbitqueues.append(config.SERVICE_ACCOUNT_CREATION)
        #rabbitqueues.append(config.SERVICE_BULK)
        rabbitmq_utils.init()
        rabbitmq_utils.createQueues(rabbitqueues)
    else: #must be kafka
        LOGGER.info("KAFKA mode")
        kafka_utils.init()

def createQueue(queueName):
    rabbitmq_utils.createQueues([queueName])

def sendToQueuer(message, topic):
    if (config.QUEUING_SYSTEM == config.QUEUING_CLOUDAMQP):
        rabbitmq_utils.sendToRabbit(message, topic)
    else:
        kafka_utils.sendToKafka(message, topic=topic)

def sendNotification(notificationContent):
    if (config.QUEUING_SYSTEM == config.QUEUING_CLOUDAMQP):
        rabbitmq_utils.sendToRabbit(notificationContent, config.SERVICE_NOTIFICATION)
    else:
        kafka_utils.sendToKafka(notificationContent, config.SERVICE_NOTIFICATION)

def listenToTopic(topicName,callback):
    if (config.QUEUING_SYSTEM == config.QUEUING_CLOUDAMQP):
        createQueue(topicName)
        rabbitmq_utils.receiveMessage(topicName,callback[config.QUEUING_SYSTEM])
    else:
        kafka_utils.receiveFromKafka("subscribe",topicName,callback[config.QUEUING_SYSTEM])
