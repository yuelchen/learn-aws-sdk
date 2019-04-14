from __future__ import print_function

# importing required modules
import boto3
import logging
import json

# setting logger configurations
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# initialize function variables
sqsName = ''
msgGroupId = '' 

def S3LambdaToFifoSQS(event, context):
	eventBucket = event['Records'][0]['s3']['bucket']['name']
	eventKey = event['Records'][0]['s3']['object']['key']

	logger.info("Received event trigger from '" + eventBucket + "/" + eventKey + "'")
	sqs = boto3.resource('sqs')
	
	logger.info("Retrieving queue url with the name '" + sqsFIFOName + "'")
	queue = sqs.get_queue_by_name(
		QueueName = sqsFIFOName
	)
	
	logger.info("Sending message to queue...")
	response = queue.send_message(
		MessageBody = json.dumps(event),
		MessageGroupId = msgGroupId
	)
	
	logger.info("Success! Message ID: " + response.get('MessageId'))
	return "Exiting S3 Lambda To FIFO SQS Function"
