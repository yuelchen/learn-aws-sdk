package com.yuelchen.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;

/**
 * Triggers class contains examples of triggers for Lambda function handler. 
 * 
 * @author 	yuelchen
 * @version	1.0.0
 * @since 	2019-09-01
 */
public class Triggers {
    
    /** 
     * Private constructor.
     */
    private Triggers() {}
    
    //====================================================================================================
	
	/**
	 * S3 Event Handler; 
	 * handler path will be 'com.yuelchen.lambda.Triggers::handleS3Event'.
	 * 
	 * @param event			S3 Event object.
	 * @param context		Context object. 
	 * 
	 * @return				a String value - in this case S3 object which triggered event.
	 */
    public String handleS3Event(S3Event event, Context context) {
        String bucketName = event.getRecords().get(0).getS3().getBucket().getName();
        String objectKey = event.getRecords().get(0).getS3().getObject().getKey();
        
        //insert your code below

        //output at the end of 'successful' function execution
        return String.format("Lambda function triggered by s3://%s/%s", 
        		bucketName, objectKey);
    }

    //====================================================================================================

    /**
     * CloudWatch Rule Handler;
     * handler path will be 'com.yuelchen.lambda.Triggers::handleCloudWatchRule'.
     * 
     * @param event			CloudWatch Event object. 
     * @param context		Context object. 
     * 
     * @return				a String value - in this case information of rule id and details. 
     */
    public String handleCloudWatchRule(ScheduledEvent event, Context context) {
        String id = event.getId();
        String detail = event.getDetailType();
        
        //insert your code below
        
        //output at the end of 'successful' function execution
        return String.format("Lambda function triggered by CloudWatch rule "
        		+ "with Id '%s' described as '%s'" , id, detail);
    }

    //====================================================================================================

    /**
     * SQS Event Handler;
     * handler path will be 'com.example.lambda.Triggers::handleSQSMessage'.
     * 
     * @param event			SQS Event object.
     * @param context		Context object. 
     * 
     * @return				a String value - in this case information on record size.
     */
    //handler path com.example.lambda.Triggers::handleSQSMessage
    public String handleSQSMessage(SQSEvent event, Context context) {
        int recordSize = event.getRecords().size();
        
        //insert your code below 
        
        //output at the end of 'successful' function execution
        return String.format("Lambda function triggered by SQS Messages and "
        		+ "received a record count of '%d' out of max of 10", recordSize);
    }
}