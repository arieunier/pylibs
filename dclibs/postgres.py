from sqlalchemy import create_engine
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime 
import os , ujson, json
import uuid
from dclibs import logs, config

MANUAL_ENGINE_POSTGRES = None
DATABASE_URL = config.DATABASE_URL

LOGGER = logs.LOGGER

if (DATABASE_URL != ''):
    Base = declarative_base()
    MANUAL_ENGINE_POSTGRES = create_engine(DATABASE_URL, pool_size=30, max_overflow=0)
    Base.metadata.bind = MANUAL_ENGINE_POSTGRES
    dbSession_postgres = sessionmaker(bind=MANUAL_ENGINE_POSTGRES)
    session_postgres = dbSession_postgres()
    LOGGER.info("{} - Initialization done Postgresql ".format(datetime.now()))

def __resultToDict(result):
    arrayData =  []
    column_names = [desc[0] for desc in result.cursor.description]

    for entry in result:
        resDic = {}
        for column in column_names:
            resDic[column] = entry[column]
            
        arrayData.append(resDic)
    return {'data' : arrayData, 'columns': column_names}

def execRequest(strReq, Attributes):
    if (MANUAL_ENGINE_POSTGRES != None):
        result = MANUAL_ENGINE_POSTGRES.execute(strReq, Attributes)
        return __resultToDict(result)
    return {'data' : [], "columns": []}

def insertServiceDefinition(dictValue):
  serviceName=dictValue['ServiceName']
  serviceLabel=dictValue['ServiceLabel']
  sqlRequest = """ 
      select * from public.ServiceDefinition where
      ServiceName = %(ServiceName)s"""
  sqlResult = MANUAL_ENGINE_POSTGRES.execute(sqlRequest, {'ServiceName':dictValue['ServiceName']})
  for entry in sqlResult:
    LOGGER.error('This service already exist, delete it first')
    return 
  
  # now inserts it
  uidService = uuid.uuid4().__str__()
  sqlInsertService = """
    insert into public.ServiceDefinition(id, ServiceName, ServiceLabel, ServiceDescription) 
    values (%(uidService)s, %(ServiceName)s, %(ServiceLabel)s, %(ServiceDescription)s );
  """
  MANUAL_ENGINE_POSTGRES.execute(sqlInsertService, {'ServiceName':dictValue['ServiceName'],
  'ServiceLabel':dictValue['ServiceLabel'],
  'ServiceDescription':dictValue['ServiceDescription'],
  'uidService':uidService})
  
  # adds all children fields
  sqlInsertAttributes = """
    insert into public.ServiceAttribute(id, refServiceDefinitionId, AttributeName, AttributeLabel, AttributeDescription)
     values (%(uidServiceAttribute)s, %(refServiceDefinitionId)s, %(AttributeName)s, %(AttributeLabel)s, %(AttributeDescription)s);
  """
  for attribute in dictValue['ServiceAttributes']:
      MANUAL_ENGINE_POSTGRES.execute(sqlInsertAttributes, 
      {'uidServiceAttribute':uuid.uuid4().__str__(),
    'AttributeName':attribute['AttributeName'],
    'AttributeLabel':attribute['AttributeLabel'],
    'AttributeDescription':attribute['AttributeDescription'],
    'refServiceDefinitionId':uidService})

# INSERT DATA ON REMOTE COMPUTE TABLE
def insertRemoteCompute(JobId, RawData):
    global MANUAL_ENGINE_POSTGRES
    sqlRequest = """
    insert into public.RemoteCompute(JobId, CreatedById, ContextRecordId__c, ComputeService__c,ComputeAttributes__c,RawData) values 
    (%(JobId)s, %(CreatedById)s, %(ContextRecordId__c)s, %(ComputeService__c)s, %(ComputeAttributes__c)s, %(RawData)s) 
    """
    attributes = {
        'JobId' : JobId,
        'RawData' : ujson.dumps(RawData),
        'CreatedById' : RawData['data']['payload']['CreatedById'],
        'ContextRecordId__c' : RawData['data']['payload']['ContextRecordId__c'],
        'ComputeService__c' : RawData['data']['payload']['ComputeService__c'],
        'ComputeAttributes__c' : ujson.dumps(RawData['data']['payload']['ComputeAttributes__c']),
      }
    LOGGER.info(attributes)
    if (MANUAL_ENGINE_POSTGRES != None):
        MANUAL_ENGINE_POSTGRES.execute(sqlRequest, attributes)

def insertRemoteComputeStep(JobId, ServiceName, CurrentText, RawData):
    global MANUAL_ENGINE_POSTGRES
    sqlRequest = """
    insert into public.RemoteComputeStep(id, refJobId, ServiceName,CurrentText, RawData ) values 
    (%(id)s, %(refJobId)s, %(ServiceName)s, %(CurrentText)s, %(RawData)s) 
    """
    attributes = {
        'id': uuid.uuid4().__str__(),
        'refJobId' : JobId,
        'RawData' : ujson.dumps(RawData),
        'ServiceName' :ServiceName,
        'CurrentText' :CurrentText  
      }
    LOGGER.info(attributes)
    if (MANUAL_ENGINE_POSTGRES != None):
        MANUAL_ENGINE_POSTGRES.execute(sqlRequest, attributes)

