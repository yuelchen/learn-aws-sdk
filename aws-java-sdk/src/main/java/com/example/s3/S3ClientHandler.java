package com.example.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.model.inventory.*;
import java.io.*;
import java.util.Map;

public class S3ClientHandler {

    private static AmazonS3 client = AmazonS3ClientBuilder.defaultClient();

    public static String getObjectContent(String bucketName, String objectKey) throws AmazonS3Exception, IOException {
        S3Object s3Object = client.getObject(bucketName, objectKey);
        S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent();

        InputStreamReader inputStreamReader = new InputStreamReader(s3ObjectInputStream);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

        String content = "";
        String currentLine = "";
        do {
            content += currentLine;
            currentLine = bufferedReader.readLine();
        } while(currentLine != null);

        return content;
    }

    public static InventoryConfiguration getS3InventoryConfiguration(String bucketName, String inventoryId) throws AmazonS3Exception {
        GetBucketInventoryConfigurationResult getBucketInventoryConfigurationResult = client.getBucketInventoryConfiguration(bucketName, inventoryId);
        return getBucketInventoryConfigurationResult.getInventoryConfiguration();
    }

    public static void setS3InventoryConfigurationKMS(String sourceBucketName, String sourcePrefix, String inventoryId, String accountId, String format, String frequency,
                                                   String destinationBucketName, String destinationPrefix, String destinationKeyId) throws AmazonS3Exception {
        //encryption
        ServerSideEncryptionKMS serverSideEncryptionKMS = new ServerSideEncryptionKMS();
        serverSideEncryptionKMS.withKeyId(destinationKeyId);

        //destination
        InventoryS3BucketDestination inventoryS3BucketDestination = new InventoryS3BucketDestination();
        inventoryS3BucketDestination.setBucketArn(destinationBucketName);
        inventoryS3BucketDestination.setPrefix(destinationPrefix);
        inventoryS3BucketDestination.setEncryption(serverSideEncryptionKMS);
        inventoryS3BucketDestination.setFormat(format);
        inventoryS3BucketDestination.setAccountId(accountId);

        InventoryDestination inventoryDestination = new InventoryDestination();
        inventoryDestination.setS3BucketDestination(inventoryS3BucketDestination);

        //source filters
        InventoryPrefixPredicate inventoryPrefixPredicate = new InventoryPrefixPredicate(sourcePrefix);
        InventoryFilter inventoryFilter = new InventoryFilter(inventoryPrefixPredicate);

        //schedule (daily | weekly)
        InventorySchedule inventorySchedule = new InventorySchedule().withFrequency(frequency);

        //configurations (all-in-one) with no accountability of versioning
        InventoryConfiguration inventoryConfiguration = new InventoryConfiguration();
        inventoryConfiguration.setDestination(inventoryDestination);
        inventoryConfiguration.setInventoryFilter(inventoryFilter);
        inventoryConfiguration.setSchedule(inventorySchedule);
        inventoryConfiguration.setIncludedObjectVersions(InventoryIncludedObjectVersions.Current);
        inventoryConfiguration.setEnabled(true);

        SetBucketInventoryConfigurationRequest setBucketInventoryConfigurationRequest = new SetBucketInventoryConfigurationRequest(sourceBucketName, inventoryConfiguration);
        client.setBucketInventoryConfiguration(setBucketInventoryConfigurationRequest);
    }

    public static void downloadObject(String bucketName, String objectKey, String location) throws AmazonS3Exception, IOException {
        S3Object s3Object = client.getObject(bucketName, objectKey);
        S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent();

        File file = new File(location);
        FileOutputStream fileOutputStream = new FileOutputStream(file);

        int length = 0;
        byte[] byteSize = new byte[1024];
        do {
            fileOutputStream.write(byteSize, 0, length);
            length = s3ObjectInputStream.read(byteSize);
        } while(length > 0);

        s3ObjectInputStream.close();
        fileOutputStream.close();
    }

    public static void uploadObject(String location, String bucketName, String objectKey) throws AmazonS3Exception {
        File file = new File(location);

        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, objectKey, file);
        client.putObject(putObjectRequest);
    }

    public static void uploadObjectWithMetadata(String location, String bucketName, String objectKey, Map<String, String> metadata) throws AmazonS3Exception, IOException {
        FileInputStream fileInputStream = new FileInputStream(location);

        ObjectMetadata objectMetadata = new ObjectMetadata();
        for(Map.Entry<String, String> entry : metadata.entrySet()) {
            objectMetadata.addUserMetadata(entry.getKey(), entry.getValue());
        }

        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, objectKey, fileInputStream, objectMetadata);
        client.putObject(putObjectRequest);

        fileInputStream.close();
    }

    public static void uploadObjectAES256(String location, String bucketName, String objectKey) throws AmazonS3Exception, IOException {
        FileInputStream fileInputStream = new FileInputStream(location);

        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);

        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, objectKey, fileInputStream, objectMetadata);
        client.putObject(putObjectRequest);

        fileInputStream.close();
    }

    public static void uploadObjectWithMetadataAES256(String location, String bucketName, String objectKey, Map<String, String> metadata) throws AmazonS3Exception, IOException {
        FileInputStream fileInputStream = new FileInputStream(location);

        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);

        for(Map.Entry<String, String> entry : metadata.entrySet()) {
            objectMetadata.addUserMetadata(entry.getKey(), entry.getValue());
        }

        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, objectKey, fileInputStream, objectMetadata);
        client.putObject(putObjectRequest);

        fileInputStream.close();
    }

    public static void deleteObject(String bucketName, String objectKey) throws AmazonS3Exception {
        DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(bucketName, objectKey);
        client.deleteObject(deleteObjectRequest);
    }
}