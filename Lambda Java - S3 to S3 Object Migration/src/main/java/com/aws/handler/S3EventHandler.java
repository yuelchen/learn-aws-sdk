package com.aws.handler;

import com.amazonaws.services.lambda.runtime.events.S3Event;

public class S3EventHandler {
	
	public static String getEventType(S3Event event) {
		return event.getRecords().get(0).getEventName();
	}
	
	public static String getBucketName(S3Event event) {
		return event.getRecords().get(0).getS3().getBucket().getName();
	}
	
	public static String getObjectPrefix(S3Event event) {
		return event.getRecords().get(0).getS3().getObject().getKey();
	}
}