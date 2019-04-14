from __future__ import print_function

# importing required modules
import boto3
import logging

# setting logger configurations
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# initialize function variables
awsRegion = ''
instances = ['']

def LambdaStopEC2Instances(event, context):
    ec2 = boto3.client('ec2', region_name=awsRegion)
	
    logger.info("Attempting to stop all instances: str(instances))
    ec2.stop_instances(InstanceIds=instances)
	
    logger.info("Successfully stopped all instances: str(instances))
    return "Exiting Lambda Stop EC2 Function"
