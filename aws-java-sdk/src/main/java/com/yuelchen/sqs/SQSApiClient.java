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
	
	/**
	 * Returns the queue url for the given queue name. 
	 * 
	 * @param queueName					the queue name.
	 * 
	 * @return							the queue url for the given queue name. 
	 * 
	 * @throws AmazonServiceException	thrown when call was successfully sent but SQS could not process
     * 									the requested action.
     * @throws SdkClientException		thrown when call couldn't be reached or wasn't unknown to SQS. 
	 */
	public static String getQueueUrl(String queueName) 
			throws AmazonServiceException, SdkClientException {
		return amazonSQSClient.getQueueUrl(queueName).getQueueUrl();
	}
	
	//====================================================================================================
	
	/**
	 * Creates a queue with the given queue name.
	 * 
	 * @param queueName					the name of the queue to be created.
	 * 
	 * @return							the queue url of the created queue. 
	 * 
	 * @throws AmazonServiceException	thrown when call was successfully sent but SQS could not process
     * 									the requested action.
     * @throws SdkClientException		thrown when call couldn't be reached or wasn't unknown to SQS. 
	 */
	public static String createQueue(String queueName) 
			throws AmazonServiceException, SdkClientException {
		
		CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
		CreateQueueResult createQueueResult = amazonSQSClient.createQueue(createQueueRequest);
		
		System.out.println(String.format("Successfully create queue with name '%s'", queueName));		
		return createQueueResult.getQueueUrl();
	}
	
	//====================================================================================================
	
	/**
	 * Creates a queue with the given queue name and attribute values. 
	 * 
	 * @param queueName					the name of the queue to be created.
	 * @param attributes				a mapping of attribute name and values. 
	 * 
	 * @return							the queue url of the created queue.
	 * 
	 * @throws AmazonServiceException	thrown when call was successfully sent but SQS could not process
     * 									the requested action.
     * @throws SdkClientException		thrown when call couldn't be reached or wasn't unknown to SQS. 
	 */
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
	
	/**
	 * Publishes a message to the given queue name.
	 * 
	 * @param queueName					the name of standard queue. 
	 * @param messageBody				the content to be published. 
	 * 
	 * @throws AmazonServiceException	thrown when call was successfully sent but SQS could not process
     * 									the requested action.
     * @throws SdkClientException		thrown when call couldn't be reached or wasn't unknown to SQS. 
	 */
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
	
	/**
	 * Publishes a message to the given queue name.
	 * 
	 * @param queueName					the name of standard queue. 
	 * @param message					the message with content to be published.
	 * 
	 * @throws AmazonServiceException	thrown when call was successfully sent but SQS could not process
     * 									the requested action.
     * @throws SdkClientException		thrown when call couldn't be reached or wasn't unknown to SQS.
	 */
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
	
	/**
	 * Publishes a message to the given queue name.
	 * 
	 * @param queueName					the name of FIFO queue. 
	 * @param messageBody				the content to be published.
	 * @param id						the group Id to be specified (required for FIFO queue). 
	 * 
	 * @throws AmazonServiceException	thrown when call was successfully sent but SQS could not process
     * 									the requested action.
     * @throws SdkClientException		thrown when call couldn't be reached or wasn't unknown to SQS.
	 */
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
	
	/**
	 * Publishes a message to the given queue name. 
	 * 
	 * @param queueName					the name of FIFO queue. 
	 * @param message					the message with content to be published.
	 * @param id						the group Id to be specified (required for FIFO queue). 
	 * 
	 * @throws AmazonServiceException	thrown when call was successfully sent but SQS could not process
     * 									the requested action.
     * @throws SdkClientException		thrown when call couldn't be reached or wasn't unknown to SQS.
	 */
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
	
	/**
	 * Returns a list of messages from polling queue with given name. 
	 * 
	 * @param queueName					the queue to be polled for messages.
	 * @param maxNumberOfMessages		the max number of messages preferred (does not guarantee). 
	 * 
	 * @return							a list of messages.
	 * 
	 * @throws AmazonServiceException	thrown when call was successfully sent but SQS could not process
     * 									the requested action.
     * @throws SdkClientException		thrown when call couldn't be reached or wasn't unknown to SQS.
	 */
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
	
	/**
	 * Deletes the message corresponding to the given receiptHandle from given queue. 
	 * 
	 * @param queueName					the queue name.
	 * @param receiptHandle				the receipt handle for message.
	 * 
	 * @throws AmazonServiceException	thrown when call was successfully sent but SQS could not process
     * 									the requested action.
     * @throws SdkClientException		thrown when call couldn't be reached or wasn't unknown to SQS.
	 */
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
	
	/**
	 * Deletes the message from given queue. 
	 * 
	 * @param queueName					the queue name.
	 * @param message					the message to be deleted.
	 * 
	 * @throws AmazonServiceException	thrown when call was successfully sent but SQS could not process
     * 									the requested action.
     * @throws SdkClientException		thrown when call couldn't be reached or wasn't unknown to SQS.
	 */
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
	
	/**
	 * Purges all messages from given queue; wait time to 60 seconds as recommended in 
	 * Amazon SQS developer documentation. 
	 * 
	 * @param queueName					the queue name. 
	 * 
	 * @throws AmazonServiceException	thrown when call was successfully sent but SQS could not process
     * 									the requested action.
     * @throws SdkClientException		thrown when call couldn't be reached or wasn't unknown to SQS.
	 */
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
	
	/**
	 * Deletes the queue with the given name. 
	 * 
	 * @param queueName					the queue name. 
	 * 
	 * @throws AmazonServiceException	thrown when call was successfully sent but SQS could not process
     * 									the requested action.
     * @throws SdkClientException		thrown when call couldn't be reached or wasn't unknown to SQS.
	 */
	public static void deleteQueue(String queueName) 
			throws AmazonServiceException, SdkClientException {
		
		amazonSQSClient.deleteQueue(queueName);	
		System.out.println(String.format("Successfully deleted queue with name '%s'", queueName));
	}
}