import boto3
import os 
from dclibs import logs, config
import requests
import uuid 

LOGGER = logs.LOGGER

BUCKETEER_AWS_ACCESS_KEY_ID=  config.BUCKETEER_AWS_ACCESS_KEY_ID
BUCKETEER_AWS_REGION= config.BUCKETEER_AWS_REGION
BUCKETEER_AWS_SECRET_ACCESS_KEY= config.BUCKETEER_AWS_SECRET_ACCESS_KEY
BUCKETEER_BUCKET_NAME= config.BUCKETEER_BUCKET_NAME



import mimetypes 


s3 = boto3.client('s3',aws_access_key_id=BUCKETEER_AWS_ACCESS_KEY_ID,aws_secret_access_key=BUCKETEER_AWS_SECRET_ACCESS_KEY)

def uploadData(localfilename, remotefilename):
    try:

        mimetype, _ = mimetypes.guess_type(localfilename)
        if mimetype is None:
            raise Exception("Failed to guess mimetype")
    
        bucket_name = BUCKETEER_BUCKET_NAME
        s3.upload_file(localfilename, bucket_name, 'public/' + remotefilename, ExtraArgs={'ContentType': mimetype})
        url ="https://"+BUCKETEER_BUCKET_NAME + '.s3.' + BUCKETEER_AWS_REGION + '.amazonaws.com/' + 'public/' + remotefilename
        LOGGER.info(url)
        return url
    except Exception as e:
        import traceback
        traceback.print_exc()
        return None

def getData(url):
    file_name = '/tmp/' + url.split('/')[-1]
    myfile = requests.get(url)

    open(file_name, 'wb').write(myfile.content)
    return file_name
