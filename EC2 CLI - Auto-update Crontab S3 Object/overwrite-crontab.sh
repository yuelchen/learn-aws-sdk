#!/bin/bash 
#
# @author: yuelchen
# @description: bash script to 
#               (1) append or overwrite crontab expression
#               (2) restart service crontab
# @date: 06/02/2020
# @resource(s): [Crontab Man Page - crontab expressions](https://man7.org/linux/man-pages/man5/crontab.5.html)

# logger file - fixed location - updated as desired
LOG_FILE_LOCATION=/var/log/custom-awslogs-update.log

# temporary file - fixed location - update as desired
TMP_DOWNLOAD_LOCATION=/tmp/download-crontab.txt
TMP_CURRENT_LOCATION=/tmp/current-crontab.txt

# variable inputs
s3BucketPrefix=

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
    echo -e "Name: custom-crontab-overwrite.sh\n"
    echo -e "Author: yuelchen\n"
    echo -e "Description:\n\t(1) append or overwrite crontab expression\n\t(2) restart service crontab\n"
    echo -e "Example: custom-crontab-overwrite.sh -b s3://bucket-name/object/prefix/file.txt -o"
}

# download crontab file from S3
function downloadCrontabFile() {
    sudo aws s3 cp ${s3BucketPrefix} ${TMP_DOWNLOAD_LOCATION}
    if [[ $? -eq 0 ]]; then 
        logInfo "Successfully downloaded crontab file from '${s3BucketPrefix}' to '${TMP_DOWNLOAD_LOCATION}'"
        formatDownloadedCrontabFile
        return 0
    else
        logError "Failed to download crontab file from '${s3BucketPrefix}' to '${TMP_DOWNLOAD_LOCATION}'"
        return 1
    fi
}

# format crontab file - remove CRLF from ASCII file as crontab will not be updatable with downloaded file
function formatDownloadedCrontabFile() {
    sudo sed -i 's/\n//g' ${TMP_DOWNLOAD_LOCATION}
    sudo sed -i 's/\r//g' ${TMP_DOWNLOAD_LOCATION}
    logInfo "Completed executing commands to remove CRLF from ASCII text file at location '${TMP_DOWNLOAD_LOCATION}'"
}

# check each line in download file if it exists in current crontab file and append if it does not exist
function appendCurrentCrontabFile() {
    sudo crontab -l > ${TMP_CURRENT_LOCATION}
    if [[ $? -eq 0 ]]; then 
        logInfo "Successfully loaded current crontab configuration to '${TMP_CURRENT_LOCATION}'"
        for line in ${TMP_DOWNLOAD_LOCATION} 
        do
            if [ ! -z $(grep "${line}" "${TMP_CURRENT_LOCATION}") ]; then
                logInfo "Line '${line}' found in '${TMP_CURRENT_LOCATION}'; skipping entry"
            else
                logInfo "Line '${line}' not found in '${TMP_CURRENT_LOCATION}'; appending"
                echo "${line}" >> ${TMP_CURRENT_LOCATION}
            fi
        done
        
        sudo rm ${TMP_DOWNLOAD_LOCATION} # remove download temporary file once completed  
        
    else 
        logError "Failed to load current crontab configuration to '${TMP_CURRENT_LOCATION}'"
    fi
}

# determine logic of append versus overwrite depending on $overwrite variable value
function overwriteAppendCrontabFile() {
    if [ $overwrite == "false" ]; then 
        logInfo "Detected overwrite to be false; attempting to append crontab with new values in download file"
        appendCurrentCrontabFile
    elif [ $overwrite == "true" ]; then
        logInfo "Detected overwrite to be true; attempting to overwrite crontab with new download file"
        sudo mv ${TMP_DOWNLOAD_LOCATION} ${TMP_CURRENT_LOCATION}
    else
        logError "Unable to update crontab expression due to unknown overwrite logic; received '${overwrite}'"
    fi
}

# overwrite crontab expression using current temporary file
function updateCrontab() {
    sudo crontab ${TMP_CURRENT_LOCATION}
    sudo rm ${TMP_CURRENT_LOCATION} # remove current temporary file once completed
}

# main function
function main() {
    downloadCrontabFile
    if [[ $? -eq 0 ]]; then
        overwriteAppendCrontabFile
        updateCrontab
    fi
    
    logInfo "Exiting custom-crontab-overwrite.sh script, see above logs for additional information"
}

while getopts b:uoh option
do 
    case $option in 
        b)  s3BucketPrefix="$OPTARG"
            ;;
        u)  overwrite="false"
            main
            ;;
        o)  overwrite="true"
            main
            ;;
        h)  printHelp
            ;;
        :)  logError "No argument given for option -b ${OPTARG}"
            ;;
        *)  logError "Invalid parameter passed, you may specify -b (s3 bucket prefix value), -o option for overwrite, -u option for update, or -h (prints help menu)"
            ;;
done
