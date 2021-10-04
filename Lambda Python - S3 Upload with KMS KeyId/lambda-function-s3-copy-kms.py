from __future__ import print_function

import json
import boto3
import logging
import ntpath;

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handleEvent(event, context):
	#Get Source Information
    sourceBucketName = event['Records'][0]['s3']['bucket']['name']
    sourceObjectKey = event['Records'][0]['s3']['object']['key']
    logger.info("Source: " + sourceBucketName + "/" + sourceObjectKey)
	
    #Initialize params
    s3 = boto3.resource('s3', 'us-east-1')
    targetBucketName = ''
    targetObjectKey = '' + ntpath.basename(sourceObjectKey)
    logger.info("Target: " + targetBucketName + "/" + targetObjectKey)
	
    #Assign Source)
    copySource = {
        'Bucket': sourceBucketName,
        'Key': sourceObjectKey
    }
    
    s3.meta.client.copy_object(CopySource=copySource, Bucket=targetBucketName, Key=targetObjectKey, ServerSideEncryption='aws:kms', SSEKMSKeyId='')
    logger.info("Successfully copied from to new S3 Bucket with KMS Key")
    return "Successfully copied from to new S3 Bucket with KMS Key"
