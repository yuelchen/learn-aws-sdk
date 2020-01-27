# aws-development 
@Description: Code repository for source code written specifically for running on AWS or personal projects heavily using AWS SDK.
@Author: Yue Chen
@Last Updated: January 27th, 2020

### AWS SKD Supported Languages (as of Jan 27th 2020)
_The programming languages marked with an asterisk (*) are used within this repository._
* C++
* Go
* Java*
* Javascript
* .NET
* Node.js*
* PHP
* Python*
* Ruby

### Getting AWS SDK
Go to [AWS Amazon](https://aws.amazon.com/tools/) and get the SDK for your IDE (Integrated Development Environment) - there are many free ones, popularly Eclipse and IntelliJ. 

Personally I prefer Eclipse since it's the IDE I started with and IntelliJ didn't exist at the time, but if you are new to software development or using Java IDE, I would recommend going with the latter mentioned option. 

### Directory Structure (unordered)
The directory structure of this repository will be broken by AWS services. Some projects or source code may use multiple services, in which case, they will be placed under the AWS service they interact most. For example, if we have an S3 Event Trigger to execture an AWS Lambda Function which executes our code for adding a new step to EMR, this project (or source code) will be placed under EMR. Not S3 since that is part of the 'infrastructure' and not Lambda as it's only serving as the runtime. 
* EC2
* EMR
* Lambda
