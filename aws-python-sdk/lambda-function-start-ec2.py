from __future__ import print_function

# importing required modules
import boto3
import logging

# setting logger configurations
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# initialize function variables
awsRegion = ''   #string value of aws region (US East - us-east-1)
instances = [''] #array of instance id's

def startEC2Instances(event, context):
    ec2 = boto3.client('ec2', region_name=awsRegion)
	
    logger.info("Attempting to start all instances: str(instances))
    ec2.start_instances(InstanceIds=instances)
	
    logger.info("Successfully started all instances: str(instances))
    return "Exiting Lambda Start EC2 Function"
	
