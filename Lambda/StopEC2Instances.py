from __future__ import print_function

# importing required modules
import boto3
import logging

# setting logger configurations
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# initialize function variables
awsRegion = ''   #string value of aws region (i.e. us-west-1, us-east-1, etc...)
instances = [''] #array of instance id's

def stopEC2Instnaces(event, context):
    ec2 = boto3.client('ec2', region_name=awsRegion)
	
    logger.info("Attempting to stop all instances: str(instances))
    ec2.stop_instances(InstanceIds=instances)
	
    logger.info("Successfully stopped all instances: str(instances))
    return "Exiting Lambda Stop EC2 Function"
