import os
import logging
import boto3
import urllib
import traceback
import json

from botocore.exceptions import ClientError
from io import BytesIO, StringIO
from gzip import GzipFile
from json import loads, dumps
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from datetime import datetime
# from botocore.vendored import requests


# Set up logging
logging.basicConfig(level=logging.DEBUG,format='%(levelname)s: %(asctime)s: %(message)s')
logger = logging.getLogger()

def getObject(bucket_name, object_name):

    # Retrieve the object
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket_name, Key=object_name)
    except ClientError as e:
        # AllAccessDisabled error == bucket or object not found
        logging.error(e)
        return None
    # Return an open StreamingBody object
    return response['Body']

def aws_session(role_arn=None, session_name='my_session'):
    if role_arn:
        client   = boto3.client('sts')
        response = client.assume_role(RoleArn=role_arn, RoleSessionName=session_name)
        session  = boto3.Session(
            aws_access_key_id     = response['Credentials']['AccessKeyId'],
            aws_secret_access_key = response['Credentials']['SecretAccessKey'],
            aws_session_token     = response['Credentials']['SessionToken'])
        return session
    else:
        return boto3.Session

def createIndice(es,indexName):
    try:
        ## Create Indice
        idx = es.indices.create(index=indexName, ignore=400)
        return idx
    except ClientError as e:
        logging.error(e)
        return e

def putIndiceSettings(es,settings,indexName):
    try:
        idx_setting = es.indices.put_settings(
            index=indexName,
            body=settings
        )
        return idx_setting
    except ClientError as e:
        logging.error(e)
        return e

def putIndiceMapping(es,mappings,indexName):
    try:
        idx_mapping = es.indices.put_mapping(
            index=indexName,
            body=mappings
        )
        return idx_mapping
    except ClientError as e:
        logging.error(e)
        return e


def createESSesion(awsauth):
    try:
        es = Elasticsearch(
            hosts=[{'host': os.environ['ES_HOST'], 'port': 443}],
            http_auth=awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection )
        return es
    except ClientError as e:
        logger.error(e)
        return e

def loadIntoES(es,jsonPayload,settings,indexName):

    try:
        # session_regular = aws_session()
        # print(session.client('sts').get_caller_identity()['Account'])
        # print(session_regular.client('sts').get_caller_identity()['Account'])
        # create an index in elasticsearch, ignore status code 400 (index already exists)
        response = es.index(index=indexName, doc_type='_doc', id=jsonPayload["eventID"], body=jsonPayload)
        return response
    except ClientError as e:
        logging.error(e)
        return None


def lambda_handler(event, context):

    aws_account_id = context.invoked_function_arn.split(":")[4]
    indexName      = 'ct-' + os.environ['INDEX_PREFIX'] + '-' + aws_account_id + '-' + datetime.now().strftime("%Y%m%d")
    
    ## ES Settings
    settings = {
        'settings': {
            'index.mapping.total_fields.limit': 100000
        }
    }
    
    for filename in os.listdir("./"):
        if filename.endswith("mappings.json"):
            f = open(filename)
            mappings = f.read()

    # Vars
    session     = aws_session(role_arn=os.environ['MASTER_ROLE_ARN'], session_name='lambda-es')
    credentials = session.get_credentials()
    region      = os.environ['AWS_REGION']
    awsauth     = AWS4Auth(credentials.access_key, credentials.secret_key, region, 'es', session_token=credentials.token)
    
    # Connect to the Elasticsearch server
    es = createESSesion(awsauth)
    if not es.indices.exists(index=indexName):
            idx = createIndice(es,indexName)
            print(idx["acknowledged"])
            if idx["acknowledged"]:
                idx_setting = putIndiceSettings(es,settings,indexName)
            else:
                logger.error("Unable to create Indice")
            
            if idx_setting["acknowledged"]:
                idx_mapping = putIndiceMapping(es,mappings,indexName)
            else:
                logger.error(idx_mapping)
    
    for record in event["Records"]:
        messages = json.loads(record["Sns"]["Message"])
        
        s3_bucket_name = messages["Records"][0]["s3"]["bucket"]["name"]
        s3_bucket_key  = messages["Records"][0]["s3"]["object"]["key"]

        if 'AWSLogs' in s3_bucket_key:
            print(s3_bucket_key)
            stream = getObject(s3_bucket_name, s3_bucket_key)
            
            if stream is not None:
                # Read first chunk of the object's contents into memory as bytes
                content = GzipFile(None, 'rb', fileobj=BytesIO(stream.read())).read().decode('utf-8')

                # Load Payload in ES
                for rec in loads(content)['Records']:
                    # ES Load
                    response = loadIntoES(es,rec,settings,indexName)
                    logger.info(response)
