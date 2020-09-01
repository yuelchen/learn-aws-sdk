package com.fanniemae.sample.lambda;

import org.json.JSONException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

public class LambdaFunction implements RequestHandler<S3Event, String>{
	//Set Global Class Variables
	@SuppressWarnings("unused")
	private String ACCESS_KEY_ID = "";
	
	@SuppressWarnings("unused")
	private String SECRET_ACCESS_KEY = "";
	
	private String AWS_REGION = "";
	private String EMR_CLUSTER_ID = "";
	private String EMR_STEP_NAME = "";
	
	private AWSCredentials awsCredentials;
    private AmazonElasticMapReduce emrClient;
    private AmazonS3 s3Client;
    private String bucketName;
    private String objectKey;
    
    public LambdaFunction(){
    	this.awsCredentials = new BasicAWSCredentials(this.getAccessKeyId(), this.getSecretAccessKey());
    	this.emrClient = AmazonElasticMapReduceClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(this.awsCredentials)).withRegion(this.AWS_REGION).build();
    	this.s3Client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(this.awsCredentials)).withRegion(this.AWS_REGION).build();
    }
    
    @Override
    public String handleRequest(S3Event event, Context context){
        context.getLogger().log("Recieved trigger event with message: " + event.toString());
        try{
        	this.bucketName = event.getRecords().get(0).getS3().getBucket().getName();
        	this.objectKey = event.getRecords().get(0).getS3().getObject().getKey();
        }
        catch(JSONException e){
        	context.getLogger().log("Met JSONException when retrieving Bucket Name and Object Key from S3 Event Message: " + e.getMessage());
        }
        
        try{
            S3Object response = s3Client.getObject(new GetObjectRequest(this.bucketName, this.objectKey));
            String contentType = response.getObjectMetadata().getContentType();
            context.getLogger().log("CONTENT TYPE: " + contentType);
            
            HadoopJarStepConfig hadoopJarStepConfig = new HadoopJarStepConfig().withJar("");
            StepConfig stepConfig = new StepConfig(this.EMR_STEP_NAME, hadoopJarStepConfig);
            AddJobFlowStepsResult result = this.emrClient.addJobFlowSteps(new AddJobFlowStepsRequest().withJobFlowId(this.EMR_CLUSTER_ID).withSteps(stepConfig));
            context.getLogger().log("Result TYPE: " + result.toString());
            
            context.getLogger().log("Successfully submitted Job to EMR: " + contentType);
        } 
        catch(Exception e){
            e.printStackTrace();
            context.getLogger().log(String.format("Error getting object %s from bucket %s. Make sure they exist and" + " your bucket is in the same region as this function.", this.objectKey, this.bucketName));
            context.getLogger().log("Exception Message: " + e);
        }
        
        return "Exiting Lambda Step to EMR Function.";
    }
    
    //Setter Methods - optional
    public void setAccessKeyId(String id){
    	this.ACCESS_KEY_ID = id;
    }
    
    public void setSecretAccessKey(String key){
    	this.SECRET_ACCESS_KEY = key;
    }
    
    //Getter Methods 
    private String getAccessKeyId(){
    	return this.getAccessKeyId();
    }
    
    private String getSecretAccessKey(){
    	return this.getSecretAccessKey();
    }
}