package com.ylc.main;

import org.apache.commons.lang3.RandomStringUtils;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.aws.handler.S3ClientHandler;
import com.aws.handler.S3EventHandler;
import com.ylc.log.AppLog;

public class S3ToS3Lambda {	
	private final String ENV_BUCKET = "DESTINATION_BUCKET";
	private final String ENV_PREFIX = "DESTINATION_PREFIX";
	private final String TEMP_LOCATION = "/tmp/";
	
	public String handleRequest(S3Event event, Context context) {
		String eventType = S3EventHandler.getEventType(event);
		String sourceBucket = S3EventHandler.getBucketName(event);
		String sourcePrefix = S3EventHandler.getObjectPrefix(event);		
		AppLog.log.info("S3ToS3Lambda triggered by event type '" + eventType + "' from '" + this.getFullS3Path(sourceBucket, sourcePrefix) + "'");
		
		String destinationBucket = System.getenv(ENV_BUCKET);
		String destinationPrefix = System.getenv(ENV_PREFIX);
		AppLog.log.info("Retrieved environmental variables for destination bucket '" + destinationBucket + "' and destination prefix '" + destinationPrefix + "'");
		
		this.transferSourceFile(sourceBucket, sourcePrefix, destinationBucket, destinationPrefix);
		
		return "End of S3ToS3Lambda function";
	}
	
	private void transferSourceFile(String sourceBucket, String sourcePrefix, String destinationBucket, String destinationPrefix) {
		String downloadLocation = this.getRandomFilenameLocation();
		if(S3ClientHandler.downloadObject(sourceBucket, sourcePrefix, downloadLocation)) {
			AppLog.log.info("Successfully copied S3 object from '" + this.getFullS3Path(sourceBucket, sourcePrefix) + "' to local location '" + downloadLocation + "'");
			this.copySourceFile(downloadLocation, destinationBucket, destinationPrefix);
		} 
	}
	
	private String getRandomFilenameLocation() {
		String randomFilename = RandomStringUtils.randomAlphabetic(10);
		return this.TEMP_LOCATION.concat(randomFilename);
	}
	
	private void copySourceFile(String downloadLocation, String destinationBucket, String destinationPrefix) {
		if(S3ClientHandler.uploadObject(downloadLocation, destinationBucket, destinationPrefix)) {
			AppLog.log.info("Successfully copied S3 object from local location '" + downloadLocation + "' to '" + this.getFullS3Path(destinationBucket, destinationPrefix) + "'");
		} 
	}
	
	private String getFullS3Path(String bucketName, String objectPrefix) {
		return "s3://" + bucketName + "/" + objectPrefix;
	}
}
