import uuid
import requests
import ujson 
from simple_salesforce import Salesforce, SalesforceLogin
import time
from libs import logs, config, utils
LOGGER = logs.LOGGER

session_id = ''
instance = ''
sfurl = ''

import jwt
import requests
import datetime
from simple_salesforce.exceptions import SalesforceAuthenticationFailed

def jwt_login(consumer_id, username, private_key, sandbox=False):
    global session_id, instance, sfurl
    LOGGER.info("consumer_id="+consumer_id)
    LOGGER.info("username="+username)

    endpoint = 'https://test.salesforce.com' if sandbox is True else 'https://login.salesforce.com'
    jwt_payload = jwt.encode(
        { 
            'exp': datetime.datetime.utcnow() + datetime.timedelta(seconds=config.JWTTOKEN_TTL),
            'iss': consumer_id,
            'aud': endpoint,
            'sub': username
        },
        private_key,
        algorithm='RS256'
    )

    result = requests.post(
        endpoint + '/services/oauth2/token',
        data={
            'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
            'assertion': jwt_payload
        }
    )
    body = result.json()
    LOGGER.info(body)
    
    if result.status_code != 200:
        raise SalesforceAuthenticationFailed(body['error'], body['error_description'])
    # now set things properly
    session_id = body['access_token']
    instance = body['instance_url']
    sfurl = instance
    
    #sf = Salesforce(instance_url=body['instance_url'], session_id=body['access_token'])
    #return sf
    return body


def sf_executeQuery(instance_url, access_token, username, soql_request):
    url = instance_url + '/services/data/v47.0/query'
    headers = {'Authorization': "Bearer " + access_token, "X-Prettylogger.debug": "1", "Content-Type" : "application/json"}
    attributes = {'q':soql_request}

    result = requests.get(url, headers=headers, params=attributes)
    LOGGER.info(result)
    return result

def sf_ChatterPost(instance_url, access_token, recordid, userid, text):
    url = instance_url + '/services/data/v45.0/chatter/feed-elements'
    
    headers = {'Authorization': "Bearer " + access_token, "X-Prettylogger.debug": "1", "Content-Type" : "application/json"}
    attributes = {}
    
    body = { 
            "body" : {
                "messageSegments" : [
                    {
                        "type" : "Text",
                        "text" : text
                    },
                    {   
                        "type" : "Mention",
                        "id" : userid
                    }]
                },
            "feedElementType" : "FeedItem"            
            }
    if (recordid != "" and recordid != None):
        body['subjectId'] = recordid
    else:
        body['subjectId'] = "me"

    data=ujson.dumps(body)
    LOGGER.info(data)
    result = requests.post(url, data=data , headers=headers, params=attributes)
    LOGGER.info(result)

def bulkv2Ingest_CreateJob(instance_url, access_token, sfobject):

    url = instance_url + "/services/data/v48.0/jobs/ingest/"
    body = {
         "object" : sfobject,
        "contentType" : "CSV",
        "operation" : "insert"
        }

    headers = {'Authorization': "Bearer " + access_token, "X-Prettylogger.debug": "1", "Content-Type" : "application/json"}        
    data=ujson.dumps(body)
    LOGGER.info("##### CREATE JOB #####")
    LOGGER.info(data)
    LOGGER.info(url)        
    LOGGER.info(headers)
    result = requests.post(url, data=data , headers=headers)
    LOGGER.info(result.json())
    return result.json()

def Bulkv2_CheckJobCompletion(instance_url, access_token, jobId, jobType):
    # now waits for the job to finish
    isFinished = False
    while (isFinished == False):
        result = bulkv2Ingest_CheckJobCompletion(instance_url, access_token, jobId, jobType)
        if (result['state'] == 'JobComplete'):
            LOGGER.info("Job is complete, stopping")
            isFinished = True
        else:
            LOGGER.info("Job is not complete, sleeping for 10s and trying again")
            time.sleep(10)
    if (jobType == 'query'):
        return result['numberRecordsProcessed']
    return ""


def bulkv2Ingest_InsertData(instance_url, access_token, jobid, data):
    url = instance_url + "/services/data/v48.0/jobs/ingest/" + jobid + "/batches/"
    headers = {'Authorization': "Bearer " + access_token, "X-Prettylogger.debug": "1", "Content-Type" : "text/csv","Accept": "application/json"}        
    LOGGER.info("##### UPLOAD DATA #####")
    LOGGER.info(url)        
    LOGGER.info(headers)
    result = requests.put(url, data=data , headers=headers)
    LOGGER.info(result.status_code)
    LOGGER.info(result.text)

def bulkv2Ingest_CloseJob(instance_url, access_token, jobid):
    url = instance_url + "/services/data/v48.0/jobs/ingest/" + jobid
    body = {
      "state" : "UploadComplete"
        }
    data=ujson.dumps(body)
    headers = {'Authorization': "Bearer " + access_token, "X-Prettylogger.debug": "1", "Content-Type" : "application/json", "Accept": "application/json"}            
    LOGGER.info("##### CLOSE JOB #####")
    LOGGER.info(url)        
    LOGGER.info(headers)
    result = requests.patch(url, data=data , headers=headers)
    LOGGER.info(result.status_code)
    LOGGER.info(result.json())
    return result.json()

def bulkv2Ingest_CheckJobCompletion(instance_url, access_token, jobid, jobtype):
    url = instance_url + "/services/data/v48.0/jobs/" + jobtype + "/" + jobid
    body = {
      "state" : "UploadComplete"
        }
    data=ujson.dumps(body)
    headers = {'Authorization': "Bearer " + access_token, "X-Prettylogger.debug": "1", "Content-Type" : "application/json", "Accept": "application/json"}            
    LOGGER.info("##### CHECK JOB #####")
    LOGGER.info(url)        
    LOGGER.info(headers)
    result = requests.get(url, data=data , headers=headers)
    LOGGER.info(result.status_code)
    LOGGER.info(result.json())
    return result.json()

def Bulkv2_INSERT(fileName, CSV_HEADER, SF_RECORD_NAME):
    
    jwt_token=utils.get_jwt_token()
    instance_url = jwt_token['instance_url']
    access_token = jwt_token['access_token']
    data = open(fileName, 'r').read()

    bulkv2_batch = bulkv2Ingest_CreateJob(instance_url,access_token, SF_RECORD_NAME)
    bulkid = bulkv2_batch['id']


    bulkv2Ingest_InsertData(instance_url, access_token, bulkid, data)
    bulkv2Ingest_CloseJob(instance_url, access_token, bulkid)
    Bulkv2_CheckJobCompletion(instance_url, access_token, bulkid, "ingest")    