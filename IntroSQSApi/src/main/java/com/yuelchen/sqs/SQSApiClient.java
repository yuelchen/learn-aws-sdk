package com.yuelchen.sqs;

import java.util.List;
import java.util.Map;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

/**
 * Amazon SQS API client handler for performing SQS operations. 
 * 
 * @author 	yuelchen
 * @version	1.0.0
 * @since 	2019-09-01
 */
public class SQSApiClient {
	
	/**
	 * The default Amazon SQS Client. 
	 */
	private static AmazonSQS amazonSQSClient = AmazonSQSClientBuilder.defaultClient();
	
	//====================================================================================================
	
	public static String getQueueUrl(String queueName) 
			throws AmazonServiceException, SdkClientException {
		return amazonSQSClient.getQueueUrl(queueName).getQueueUrl();
	}
	
	//====================================================================================================
	
	public static String createQueueWithAttributes(String queueName) 
			throws AmazonServiceException, SdkClientException {
		
		CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
		CreateQueueResult createQueueResult = amazonSQSClient.createQueue(createQueueRequest);
		
		System.out.println(String.format("Successfully create queue with name '%s'", queueName));		
		return createQueueResult.getQueueUrl();
	}
	
	//====================================================================================================
	
	public static String createQueueWithAttributes(String queueName, Map<String, String> attributes) 
			throws AmazonServiceException, SdkClientException {
		
		CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName)
				.withAttributes(attributes);
		CreateQueueResult createQueueResult = amazonSQSClient.createQueue(createQueueRequest);
		
		System.out.println(String.format("Successfully create queue with name '%s' and '%d' "
				+ "attributes", queueName, attributes.size()));
		return createQueueResult.getQueueUrl();
	}
	
	//====================================================================================================
	
	public static void publishStandardMessage(String queueName, String messageBody) 
			throws AmazonServiceException, SdkClientException {
		
		String queueUrl = getQueueUrl(queueName);
		SendMessageRequest sendMessageRequest = new SendMessageRequest()
				.withQueueUrl(queueUrl)
				.withMessageBody(messageBody);
		
		SendMessageResult sendMessageResult = amazonSQSClient.sendMessage(sendMessageRequest);
		System.out.println(String.format("Successfully published message to standard queue with url "
				+ "'%s' and recieved message Id '%s'", queueUrl, sendMessageResult.getMessageId()));
	}
	
	//====================================================================================================
	
	public static void publishStandardMessage(String queueName, Message message) 
			throws AmazonServiceException, SdkClientException {
		
		String queueUrl = getQueueUrl(queueName);
		SendMessageRequest sendMessageRequest = new SendMessageRequest()
				.withQueueUrl(queueUrl)
				.withMessageBody(message.getBody());
		
		SendMessageResult sendMessageResult = amazonSQSClient.sendMessage(sendMessageRequest);
		System.out.println(String.format("Successfully published message to standard queue with url "
				+ "'%s' and recieved message Id '%s'", queueUrl, sendMessageResult.getMessageId()));
	}
	
	//====================================================================================================
	
	public static void publishFIFOMessage(String queueName, String messageBody, String id) 
			throws AmazonServiceException, SdkClientException {
		
		String queueUrl = getQueueUrl(queueName);
		SendMessageRequest sendMessageRequest = new SendMessageRequest()
				.withQueueUrl(queueUrl)
				.withMessageBody(messageBody)
				.withMessageGroupId(id);
		
		SendMessageResult sendMessageResult = amazonSQSClient.sendMessage(sendMessageRequest);
		System.out.println(String.format("Successfully published message to FIFO queue with url "
				+ "'%s' and recieved message Id '%s'", queueUrl, sendMessageResult.getMessageId()));
	}
	
	//====================================================================================================
	
	public static void publishFIFOMessage(String queueName, Message message, String id) 
			throws AmazonServiceException, SdkClientException {
		
		String queueUrl = getQueueUrl(queueName);
		SendMessageRequest sendMessageRequest = new SendMessageRequest()
				.withQueueUrl(queueUrl)
				.withMessageBody(message.getBody())
				.withMessageGroupId(id);
		
		SendMessageResult sendMessageResult = amazonSQSClient.sendMessage(sendMessageRequest);
		System.out.println(String.format("Successfully published message to FIFO queue with url "
				+ "'%s' and recieved message Id '%s'", queueUrl, sendMessageResult.getMessageId()));
	}
	
	//====================================================================================================
	
	public static List<Message> pollMessage(String queueName, int maxNumberOfMessages) 
			throws AmazonServiceException, SdkClientException {
		
		String queueUrl = getQueueUrl(queueName);
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
				.withQueueUrl(queueUrl)
				.withMaxNumberOfMessages(maxNumberOfMessages);
		
		ReceiveMessageResult receiveMessageResult = 
				amazonSQSClient.receiveMessage(receiveMessageRequest);
		System.out.println(String.format("Successfully polled '%d' messages from queue with url "
				+ "'%s'", receiveMessageResult.getMessages().size() ,queueUrl));
		return receiveMessageResult.getMessages();
	}
	
	//====================================================================================================
	
	public static void deleteMessage(String queueName, String receiptHandle) 
			throws AmazonServiceException, SdkClientException {
		
		String queueUrl = getQueueUrl(queueName);
		DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest()
				.withQueueUrl(queueUrl)
				.withReceiptHandle(receiptHandle);
		
		amazonSQSClient.deleteMessage(deleteMessageRequest);
		System.out.println(String.format("Successfully deleted message with receipt handler "
				+ "'%s' from queue with url '%s'", receiptHandle, queueUrl));
	}
	
	//====================================================================================================
	
	public static void deleteMessage(String queueName, Message message) 
			throws AmazonServiceException, SdkClientException {
		
		String queueUrl = getQueueUrl(queueName);
		DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest()
				.withQueueUrl(queueUrl)
				.withReceiptHandle(message.getReceiptHandle());
		
		amazonSQSClient.deleteMessage(deleteMessageRequest);
		System.out.println(String.format("Successfully deleted message with receipt handler '%s' "
				+ "from queue with url '%s'", message.getReceiptHandle(), queueUrl));
	}
	
	//====================================================================================================
	
	public static void purgeQueueMessages(String queueName) 
			throws AmazonServiceException, SdkClientException {
		
		String queueUrl = getQueueUrl(queueName);
		PurgeQueueRequest purgeQueueRequest = new PurgeQueueRequest()
				.withQueueUrl(queueUrl);
		
		amazonSQSClient.purgeQueue(purgeQueueRequest);
		try {
			Thread.sleep(60000);
			System.out.println(String.format("Successfully purged queue with name '%s' and waited "
					+ "for 60 seconds as recommended by Amazon in Java SDK Docs", queueName));
		} catch(InterruptedException e) {
			System.out.println(String.format("Successfully purged queue with name '%s' but failed "
					+ "to wait for 60 seconds as recommended by Amazon in Java SDK Docs", queueName));
		}
	}
	
	//====================================================================================================
	
	public static void deleteQueue(String queueName) 
			throws AmazonServiceException, SdkClientException {
		
		amazonSQSClient.deleteQueue(queueName);	
		System.out.println(String.format("Successfully deleted queue with name '%s'", queueName));
	}
}