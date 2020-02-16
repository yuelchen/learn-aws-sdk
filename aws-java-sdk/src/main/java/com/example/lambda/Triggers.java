package com.example.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;

public class Triggers {

    //handler path com.example.lambda.Triggers::handleS3Event
    public String handleS3Event(S3Event event, Context context) {
        String bucketName = event.getRecords().get(0).getS3().getBucket().getName();
        String objectKey = event.getRecords().get(0).getS3().getObject().getKey();

        //your code (function calls) here

        return "Lambda function triggered by s3://" + bucketName + "/" + objectKey;
    }

    //handler path com.example.lambda.Triggers::handleCloudWatchRule
    public String handleCloudWatchRule(ScheduledEvent event, Context context) {
        String id = event.getId();
        String detail = event.getDetailType();

        //your code (function calls) here

        return "Lambda function triggered by CloudWatch rule with Id '" + id + "' described as '" + detail + "'";
    }

    //handler path com.example.lambda.Triggers::handleSQSMessage
    public String handleSQSMessage(SQSEvent event, Context context) {
        int recordSize = event.getRecords().size();

        //your code (function calls) here

        return "Lambda function triggered by SQS Messages and received a record count of '" + recordSize + "' out of max of 10";
    }
}