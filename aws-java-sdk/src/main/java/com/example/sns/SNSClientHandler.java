package com.example.sns;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.*;
import java.util.HashMap;
import java.util.Map;

public class SNSClientHandler {
    public static AmazonSNS client = AmazonSNSClientBuilder.defaultClient();

    public static String publishMessage(String topicName, String subjectTile, String messageContent) throws AmazonSNSException {
        CreateTopicRequest createTopicRequest = new CreateTopicRequest(topicName);
        CreateTopicResult createTopicResult = client.createTopic(createTopicRequest);

        PublishRequest publishRequest = new PublishRequest();
        publishRequest.withTopicArn(createTopicResult.getTopicArn());
        publishRequest.withSubject(subjectTile);
        publishRequest.withMessage(messageContent);

       PublishResult publishResult = client.publish(publishRequest);
       return publishResult.getMessageId();
    }

    public static String publishMessageWithStringAttributes(String topicName, String subjectTile, String messageContent, Map<String, String> attributes) throws AmazonSNSException {
        Map<String, MessageAttributeValue> messageAttributeValues = new HashMap<String, MessageAttributeValue>();
        for(Map.Entry<String, String> entry : attributes.entrySet()) {
            MessageAttributeValue messageAttributeValue = new MessageAttributeValue().withDataType("String").withStringValue(entry.getValue());
            messageAttributeValues.put(entry.getKey(), messageAttributeValue);
        }

        CreateTopicRequest createTopicRequest = new CreateTopicRequest(topicName);
        CreateTopicResult createTopicResult = client.createTopic(createTopicRequest);

        PublishRequest publishRequest = new PublishRequest();
        publishRequest.withTopicArn(createTopicResult.getTopicArn());
        publishRequest.withSubject(subjectTile);
        publishRequest.withMessage(messageContent);
        publishRequest.withMessageAttributes(messageAttributeValues);

        PublishResult publishResult = client.publish(publishRequest);
        return publishResult.getMessageId();
    }
}