package com.yuelchen.sns;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.ConfirmSubscriptionRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.amazonaws.services.sns.model.UnsubscribeRequest;

/**
 * Amazon SNS API client handler for performing SNS operations. 
 * 
 * @author 	yuelchen
 * @version	1.0.0
 * @since 	2019-09-01
 */
public class SNSApiClient {
	
	/**
	 * The default Amazon SNS Client. 
	 */
	private static AmazonSNS amazonSNSClient = AmazonSNSClientBuilder.defaultClient();
	
	//====================================================================================================
    
    /** 
     * Private constructor.
     */
    private SNSApiClient() {}
    
    //====================================================================================================
    
	/**
	 * Creates a topic with the given name. 
	 * 
	 * @param topicName					the topic name.
	 * 
	 * @return							the created topic arn. 
	 * 
	 * @throws AmazonServiceException	thrown when call was successfully sent but SNS could not process
     * 									the requested action.
     * @throws SdkClientException		thrown when call couldn't be reached or wasn't unknown to SNS. 
	 */
	public static String createTopic(String topicName) 
			throws AmazonServiceException, SdkClientException {
		
		CreateTopicResult result = amazonSNSClient.createTopic(topicName);
		System.out.println(String.format("Successfully create topic with name '%s'", topicName));		
		return result.getTopicArn();
	}
	
	//====================================================================================================
	
	/**
	 * Returns the subscription arn after adding a new subscriber to given topic. 
	 * 
	 * @param topicArn					the topic arn.
	 * @param type						the subscription type (i.e. SQS, E-mail).
	 * @param endpoint					the subscription endpoint. 
	 * 
	 * @return							the subscription arn. 
	 * 
	 * @throws AmazonServiceException	thrown when call was successfully sent but SNS could not process
     * 									the requested action.
     * @throws SdkClientException		thrown when call couldn't be reached or wasn't unknown to SNS.
	 */
	public static String addSubscriberToTopic(String topicArn, String type, String endpoint) 
			throws AmazonServiceException, SdkClientException {
		
		SubscribeRequest subscribeRequest = new SubscribeRequest(topicArn, type, endpoint);
		SubscribeResult subscribeResult = amazonSNSClient.subscribe(subscribeRequest);
		System.out.println(String.format("Successfully create subscription for endpoint '%s' "
				+ "of type '%s' to topic arn '%s' and recieved subscription arn '%s'", 
				endpoint, type, topicArn,subscribeResult.getSubscriptionArn()));
		return subscribeResult.getSubscriptionArn();
	}
	
	//====================================================================================================
	
	/**
	 * Comfirms the subscription for given topic using subscription token. 
	 * 
	 * @param topicArn					the topic arn. 
	 * @param token						the subscription token. 
	 * 
	 * @throws AmazonServiceException	thrown when call was successfully sent but SNS could not process
     * 									the requested action.
     * @throws SdkClientException		thrown when call couldn't be reached or wasn't unknown to SNS.
	 */
	public static void confirmSubscription(String topicArn, String token) 
			throws AmazonServiceException, SdkClientException {
		
		ConfirmSubscriptionRequest confirmSubscriptionRequest = 
				new ConfirmSubscriptionRequest()
				.withTopicArn(topicArn)
				.withToken(token);
		amazonSNSClient.confirmSubscription(confirmSubscriptionRequest);
	}
	
	//====================================================================================================
	
	/**
	 * Removes the subscription for given subscription arn. 
	 * 
	 * @param subscriptionArn			the subscription arn. 
	 * 
	 * @throws AmazonServiceException	thrown when call was successfully sent but SNS could not process
     * 									the requested action.
     * @throws SdkClientException		thrown when call couldn't be reached or wasn't unknown to SNS.
	 */
	public static void removeSubscriberFromTopic(String subscriptionArn) 
			throws AmazonServiceException, SdkClientException {
		
		UnsubscribeRequest unsubscribeRequest = new UnsubscribeRequest()
				.withSubscriptionArn(subscriptionArn);		
		amazonSNSClient.unsubscribe(unsubscribeRequest);
		System.out.println(String.format("Successfully removed subscription with arn '%s'", 
				subscriptionArn));
	}
}