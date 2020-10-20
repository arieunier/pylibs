from libs import queuer, logs, rediscache, sfapi, config, postgres, rabbitmq_utils
import ujson
LOGGER = logs.LOGGER

def get_jwt_token():
    jwt_token = rediscache.__getCache(config.REDIS_JWTTOKEN)
    LOGGER.info(jwt_token)
    if (jwt_token == None):
        # connects
        jwt_token = sfapi.jwt_login(config.CONSUMER_KEY, config.USERNAME, config.JWTKEY, config.SANDBOX)
        LOGGER.info(jwt_token)
        # saves into cache
        rediscache.__setCache(config.REDIS_JWTTOKEN, ujson.dumps(jwt_token),config.JWTTOKEN_TTL)
    else:
        jwt_token = ujson.loads(jwt_token)
    return jwt_token


# sends the notification
def sendNotification(messageContent):
    #check if the jwt token is properly  
    jwt_token=get_jwt_token()
    
    # ok we have the token now we can send the proper notification
    LOGGER.info(jwt_token)
    sfapi.sf_ChatterPost(jwt_token['instance_url'], 
                   jwt_token['access_token'],
                   messageContent['data']['payload']['ContextRecordId__c'],
                   messageContent['data']['payload']['CreatedById'], 
                   messageContent['text'])


def serviceTracesAndNotifies(dictContent, serviceName, text, generateNotification):
    # store the fact the service received data
    postgres.insertRemoteComputeStep(dictContent['jobid'], serviceName, text, dictContent)
    if (generateNotification):
        notificationContent = dictContent
        notificationContent['text'] = text
        queuer.sendNotification(notificationContent)