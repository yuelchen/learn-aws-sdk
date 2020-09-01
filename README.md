# explore-aws-sdk-languages
This repository is for storing a collection of sources for the different languages supported by Amazon AWS SDK for developers. 

### Amazon AWS SDK Supported Languages (as of August 30th 2020)
_The programming languages marked with a speech ballon (💬) are used within this repository._
* 💬 Java
* 💬 Python
* 💬 Node.js
* 💭 C++
* 💭 Go
* 💭 Javascript
* 💭 .NET
* 💭 PHP
* 💭 Ruby

### 🏃 Getting the AWS SDK
Go to [AWS Amazon](https://aws.amazon.com/tools/) and get the SDK for your IDE (Integrated Development Environment) - there are many free ones, popularly [Eclipse](https://www.eclipse.org/ide/) and [IntelliJ](https://www.jetbrains.com/idea/) for Java development. 

Personally I prefer Eclipse since it's the IDE I started with and IntelliJ didn't exist at the time, but if you are new to software development or using Java IDE, I would recommend going with the latter mentioned option - if you choose to go with IntelliJ, the Community version is the only **FREE** option. 

For IDE's on the other AWS supported programming languages, Eclipse also supports a number of them but but since cannot say from experience, a quick [google](https://www.google.com/) search may generate better recommendations.

### 📂 Directory Structure (unordered)
_The directory structure of this repository will be broken by AWS services. Some projects or source code may use multiple services, in which case, they will be placed under the AWS service they interact most. For example, if we have an S3 Event Trigger to execture an AWS Lambda Function which executes our code for adding a new step to EMR, this project (or source code) will be placed under EMR. Not S3 since that is part of the 'infrastructure' and not Lambda as it's only serving as the runtime._
* S3
* SQS
* EC2
* EMR
* Lambda
