package com.example.sqs;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

public class SQSClientHandler {
    public static AmazonSQS client = AmazonSQSClientBuilder.defaultClient();

    public static String publishMessageContent(String queueName, String messageContent) throws AmazonSQSException {
        String queueUrl = client.getQueueUrl(queueName).getQueueUrl();

        SendMessageRequest sendMessageRequest = new SendMessageRequest();
        sendMessageRequest.setQueueUrl(queueUrl);
        sendMessageRequest.setMessageBody(messageContent);

        SendMessageResult sendMessageResult = client.sendMessage(sendMessageRequest);
        return sendMessageResult.getMessageId();
    }

    public static void deleteMessage(String queueName, Message message) throws AmazonSQSException{
        String queueUrl = client.getQueueUrl(queueName).getQueueUrl();
        client.deleteMessage(queueUrl, message.getReceiptHandle());
    }

    public static void deleteMessageWithReceiptHandle(String queueName, String receiptHandle) throws AmazonSQSException{
        String queueUrl = client.getQueueUrl(queueName).getQueueUrl();
        client.deleteMessage(queueUrl, receiptHandle);
    }
}