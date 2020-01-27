#!/bin/sh

# To set it up so this script executes as an EMR Step for executing custom jar, run the below command. 
# 
# Note: 
#  - Both your custom jar and this script will be store in an S3 bucket unless modifications are made. 
#  - Your user will need the necessary IAM policies to execute below command. 
#  - Your instance role (EMR role) will need s3:GetObjects policy for this to work, you can modify script so that way instead of downloading, it can simply execute local jar file stored on EMR. 
#
# Command: aws emr add-steps --cluster-id j-XXXXXXXX --steps Type=CUSTOM_JAR, Name=CustomJAR, ActionOnFailure=CONTINUE, Jar=s3://region.elasticmapreduce/libs/script-runner/script-runner.jar, Args=["s3://mybucket/script-path/my_script.sh"]

#script variables that are not from parameters
HOME_DIRECTORY="/home/myID"
bucketName=myBucket                       #bucket name where custom jar is located
objectKey=myObject                        #object key where custom jar is located
loggerFile=custom-jar-script-${date}.log  #log filename which will be generated at runtime of this script

function logInfo() {
  echo -e "$(date +%Y-%m-%d' '%H:%M:%S' '%Z) [INFO] $1" | tee -a "$loggerFile"
}

function logInfo() {
  echo -e "$(date +%Y-%m-%d' '%H:%M:%S' '%Z) [WARN] $1" | tee -a "$loggerFile"
}

function logInfo() {
  echo -e "$(date +%Y-%m-%d' '%H:%M:%S' '%Z) [ERROR] $1" | tee -a "$loggerFile"
}

function executeJar() {
  filename=$1
  
  #check if there is a current process already running (comment out if you want multiple processes to run at same time)
  jarPID=$(ps -ef | grep ${filename} | grep -v grep | awk '{print $2}')
  jarPIDLength=$(ps -ef | grep ${filename} | grep -v grep | awk '{print $2}' | wc -c)
  if (( jarPIDLength > 0 ));
  then
    #found already running process
    logWarn "Killing current running process with PID ${jarPID}"
    kill -9 jarPID
  fi
  
  #execute jar application
  java -jar ${HOME_DIRECTORY}/${filename} & #ampersand runs process in background
}

function downloadJar() {
  #download file from AWS S3
  filename=$1
  aws cp s3://${bucketName}/${objectKey} ${HOME_DIRECTORY}/${filename)
  
  #verify download
  jarCount=$(ls -lAtr ${HOME_DIRECTORY}/${1})
  if (( $jarCount == 1 ));
  then
    logInfo "${filename} found under ${HOME_DIRECTORY}"
    executeJar $filename
    
  else
    logInfo "Exiting script, ${filename} NOT found under ${HOME_DIRECTORY}"
    exit 1
  fi
}

function main() {
  logInfo "Starting Submit Custom Jar to EMR Script"
  if [[ $bucketName ]] && [[ $objectKey ]]; 
  then
    #we have necessary parameters needed to start, check if file exists in AWS S3
    exist=$(aws ls s3://${bucketName}/${objectKey})
    
    if [[ $exist ]];
    then
      #download file from AWS S3
      filename=$(basename ${objectKey})
      aws cp s3://${bucketName}/${objectKey} ${HOME_DIRECTORY}/${filename}
    
      #now try to download
      downloadJar $filename
    
    else
      #exit due to missing file in s3
      logError "Exiting script, s3://${bucketName}/${objectKey} does not exist in AWS S3"
      exit 1
      
    fi
    
  else
    #exit due to missing parameters
    logError "Exiting script, either bucket name or object key is missing"
    exit 1
  fi
}

main
