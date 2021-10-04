#!/bin/bash 
#
# @author: yuelchen
# @description: bash script to 
#               (1) install (if applicable) or update awslogs agent
#               (2) overwrite awslogs.conf file with given s3 awslogs.conf object prefix
#               (3) restart service awslogs agent
# @date: 05/31/2020
# @resource(s): [AWS Quick Start Guide](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/QuickStartEC2Instance.html)

# logger file - fixed location - updated as desired
LOG_FILE_LOCATION=/var/log/custom-awslogs-update.log

# fixed location for where awslogs.conf resides and used by awslogs agent 
AWSLOGS_CONFIG_LOCATION=/etc/awslogs/awslogs.conf

# logger functions [info][warn][error]
function logInfo() {
    echo "$(date +%Y-%m-%d' '%H:%M:%S' '$Z) [INFO] ${1}" >> $LOG_FILE_LOCATION
}

function logWarn() {
    echo "$(date +%Y-%m-%d' '%H:%M:%S' '$Z) [WARN] ${1}" >> $LOG_FILE_LOCATION
}

function logError() {
    echo "$(date +%Y-%m-%d' '%H:%M:%S' '$Z) [ERROR] ${1}" >> $LOG_FILE_LOCATION
}

# help output
function printHelp() {
    echo -e "Name: custom-awslogs-update.sh\n"
    echo -e "Author: yuelchen\n"
    echo -e "Description:\n\t(1) install (if applicable) or update awslogs agent\n\t(2) overwrite awslogs.conf file with given s3 awslogs.conf object prefix\n\t(3) restart service awslogs agent\n"
    echo -e "Example: custom-awslogs-update.sh -b s3://bucket-name/object/prefix/file.conf"
}

# verify whether of not awslogs is already installed
function verifyInstall() {
    isInstalledResult=$(yum list installed | grep awslogs)
    if [[ ${isInstalledResult} ]]; then
        isInstalledResults=( $isInstalledResult )
        logInfo "AWSLogs agent is already installed with version '${isInstalledResults[1]}'"
        return 0 #successfully verfied
    else
        logInfo "AWSLogs agent is not installed, attempting to install now"
        installResult=$(installAWSLogs)
        return $installResult
    fi
}

# update (linux) instance to pick up latest change to package repositories - to install latest version of awslogs
function updatePackageRepo() {
    logInfo "Updating package repository with latest version for underlying (linux) instance"
    updatePackageResult=$(sudo yum update -y)
    logInfo "${updatePackageResult}"
    
    if [[ $? -eq 0 ]]; then
        logInfo "Underlying (linux) instance successfully updated package repositories"
    else
        logWarn "Underlying (linux) instance failed to update package repositories"
    fi
}

# install AWSLOGS agent onto underlying (linux) instance
function installAWSLogs() {
    updatePackageRepo
    
    logInfo "Installing awslogs agent from Amazon AWS"
    installResult=$(sudo yum install -y awslogs)
    logInfo "${installResult}"
    
    if [[ $? -eq 0 ]]; then 
        isInstalledResult=$(yum list installed | grep awslogs)
        isInstalledResults=( $isInstalledResult )
        logInfo "Service awslogs agent was successfully installed with version '${isInstalledResults[1]}'"
        return 0
    else
        logError "Service awslogs agent could not be installed"
        return 1
    fi
}

# update AWSLOGS agent configuration file on underlying (linux) instance
function downloadAWSLogsConfiguration() {
    if [[ $1 -eq 0 ]]; then 
        logInfo "Attempting to update configuration file from S3 '${s3BucketPrefix}'"
        downloadResult=$(sudo aws s3 cp ${s3BucketPrefix} ${AWSLOGS_CONFIG_LOCATION})
        
        if [[ $? -eq 0 ]]; then
            logInfo "Configuration file for awslogs agent was successfully updated at location '${s3BucketPrefix}'"
        else
            logInfo "Configuration file for awslogs agent could not be updated at location '${s3BucketPrefix}'"
        fi
        
    else
        logError "Skipping download / update of awslogs agent configuration file due to failed installation or update"
    fi
}

# restart service awslogs agent
function restartAWSLogs() {
    if [[ $1 -eq 0 ]]; then 
        stopResult=$(sudo service awslogs stop)
        logInfo "${stopResult}"
        
        startResult=$(sudo service awslogs start)
        logInfo "${startResult}"
        if [[ $? -eq 0 ]]; then
            logInfo "Successfully restarted service awslogs agent"
        else
            logError "Failed to restart service awslogs agent"
        fi
        
    fi
}

# main function - called if s3 bucket prefix parameter is given
function main() {
    verifyInstall
    downloadAWSLogsConfiguration
    restartAWSLogs
}

while getopts b:h option
do 
    case $option in
        b)  s3BucketPrefix="$OPTARG"
            main
            ;;
        h)  printHelp
            ;;
        :)  logError "No argument given for option -b ${OPTARG}"
            ;;
        *)  logError "Invalid parameter passed, you may specify -b (s3 bucket prefix value) or -h (prints help menu)"
            ;;
    esac
done
