package com.yuelchen.s3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteBucketRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetBucketInventoryConfigurationResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.SetBucketInventoryConfigurationRequest;
import com.amazonaws.services.s3.model.inventory.InventoryConfiguration;
import com.amazonaws.services.s3.model.inventory.InventoryDestination;
import com.amazonaws.services.s3.model.inventory.InventoryIncludedObjectVersions;
import com.amazonaws.services.s3.model.inventory.InventoryOptionalField;
import com.amazonaws.services.s3.model.inventory.InventoryS3BucketDestination;
import com.amazonaws.services.s3.model.inventory.InventorySchedule;
import com.amazonaws.services.s3.model.inventory.ServerSideEncryptionKMS;
import com.amazonaws.util.IOUtils;

/**
 * Amazon S3 API client handler for performing Glue operations. 
 * 
 * @author 	yuelchen
 * @version	1.0.0
 * @since 	2019-09-01
 */
public class S3ApiClient {
	
	
	
	/**
	 * The default Amazon S3 Client for making API requests. 
	 */
	private static AmazonS3 amazonS3Client = AmazonS3ClientBuilder.defaultClient();

    //====================================================================================================
	
	public static boolean isBucketExist(String bucketName) 
			throws AmazonServiceException, SdkClientException {
		
		return amazonS3Client.doesBucketExistV2(bucketName);
	}

    //====================================================================================================
	
	public static Bucket createBucket(String bucketName) 
			throws AmazonServiceException, SdkClientException {
		
		CreateBucketRequest createBucketRequest = new CreateBucketRequest(bucketName);		
		return amazonS3Client.createBucket(createBucketRequest);
	}

    //====================================================================================================
	
	public static List<String> listPrefix(String bucketName, String objectPrefix) 
			throws AmazonServiceException, SdkClientException {
		
		ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
				.withBucketName(bucketName)
				.withPrefix(objectPrefix)
				.withDelimiter(S3ObjectParse.PREFIX_DELIMITER);
		
		List<String> prefixList = new ArrayList<String>();
		ListObjectsV2Result listObjectsResult;
		int requestCount = 1;
		do {
			
			//retrieve prefix list from response
			listObjectsResult = amazonS3Client.listObjectsV2(listObjectsRequest);
			List<String> commonPrefixes = listObjectsResult.getCommonPrefixes();
			prefixList.addAll(commonPrefixes);
			
			//check to see if there are more prefixes to paginate through
			String token = listObjectsResult.getNextContinuationToken();
			listObjectsRequest.setContinuationToken(token);
			
			//print out progress report in logger and increment request count
			System.out.print(String.format("Prefix list request count '%d' retrieved '%d' "
					+ "prefixes in response from path 's3://%s/%s with continuation token '%s''", 
					requestCount, commonPrefixes.size(), bucketName, objectPrefix, token));
			requestCount++;
			
		} while(listObjectsResult.isTruncated());
		
		return prefixList;
	}
	
	//====================================================================================================
	
	public static String getObjectContent(String bucketName, String objectPrefix) 
			throws AmazonServiceException, SdkClientException, IOException {
		
		S3Object s3Object = amazonS3Client.getObject(bucketName, objectPrefix);
		S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent();
		
		InputStreamReader inputStreamReader = new InputStreamReader(s3ObjectInputStream);
		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
		
		String content = "", line = "";
		do {
			content += line;
			line = bufferedReader.readLine();
		} while(line != null);
		
		s3ObjectInputStream.close();
		inputStreamReader.close();
		bufferedReader.close();
		
		return content;
	}
	
	//====================================================================================================
		
	public static void downloadWithFileLocation(String fileLocation, String bucketName, String objectPrefix) 
			throws AmazonServiceException, SdkClientException, IOException {
		
		S3Object s3Object = amazonS3Client.getObject(bucketName, objectPrefix);
		S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent();
		
		File outputFile = new File(fileLocation);
		FileOutputStream fileOutputStream = new FileOutputStream(outputFile);
		
		byte[] bufferSize = new byte[1024];
		int contentIndex = 0;
		do {
			fileOutputStream.write(bufferSize, 0, contentIndex);
			contentIndex = s3ObjectInputStream.read(bufferSize);
		} while(contentIndex > 0);
		
		s3ObjectInputStream.close();
		fileOutputStream.close();
	}
	
	//====================================================================================================
	
	public static String downloadWithFilename(String filename, String bucketName, String objectPrefix) 
			throws AmazonServiceException, SdkClientException, IOException {
		
		String fileLocation = "./tmp/".concat(filename);
		
		downloadWithFileLocation(fileLocation, bucketName, objectPrefix);
		return fileLocation;
	}
	
	//====================================================================================================
	
	public static String downloadDefault(String bucketName, String objectPrefix) 
			throws AmazonServiceException, SdkClientException, IOException {
		
		String filename = S3ObjectParse.getFilename(objectPrefix);
		String fileLocation = "./tmp/".concat(filename);
		
		downloadWithFileLocation(fileLocation, bucketName, objectPrefix);
		return fileLocation;
	}
	
	//====================================================================================================
	
	private static ObjectMetadata getObjectMetadata(FileInputStream fileInputStream)
			throws IOException {
		
		byte[] byteArray = IOUtils.toByteArray(fileInputStream);
		Long contentSize = Long.valueOf(byteArray.length);
		
		ObjectMetadata objectMetadata = new ObjectMetadata();
		objectMetadata.setContentLength(contentSize.longValue());
		
		return objectMetadata;
	}
	
	//====================================================================================================
	
	public static void uploadWithAES256(String fileLocation, String bucketName, String objectPrefix) 
			throws AmazonServiceException, SdkClientException, IOException {
		
		File inputFile = new File(fileLocation);
		FileInputStream fileInputStream = new FileInputStream(inputFile);
		ObjectMetadata objectMetadata = getObjectMetadata(fileInputStream);
		objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
		
		PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, objectPrefix,
				fileInputStream, objectMetadata);
		amazonS3Client.putObject(putObjectRequest);
		
		fileInputStream.close();
	}
	
	//====================================================================================================
	
	public static void uploadMetadataWithAES256(String fileLocation, String bucketName, 
			String objectPrefix, Map<String, String> keyPairs) 
					throws AmazonServiceException, SdkClientException, IOException {
		
		File inputFile = new File(fileLocation);
		FileInputStream fileInputStream = new FileInputStream(inputFile);
		ObjectMetadata objectMetadata = getObjectMetadata(fileInputStream);
		objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
		
		for(Map.Entry<String, String> keyPair : keyPairs.entrySet()) {
			objectMetadata.addUserMetadata(keyPair.getKey(), keyPair.getValue());
		}
		
		PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, objectPrefix,
				fileInputStream, objectMetadata);
		amazonS3Client.putObject(putObjectRequest);
		
		fileInputStream.close();
	}
	
	//====================================================================================================
	
	public static void uploadWithKMS(String fileLocation, String bucketName, 
			String objectPrefix, String kmsKeyId) 
					throws AmazonServiceException, SdkClientException, IOException {
		
		File inputFile = new File(fileLocation);
		FileInputStream fileInputStream = new FileInputStream(inputFile);
		ObjectMetadata objectMetadata = getObjectMetadata(fileInputStream);		
		SSEAwsKeyManagementParams sseAwsKeyManagementParams = new SSEAwsKeyManagementParams(kmsKeyId);
		
		PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, objectPrefix,
				fileInputStream, objectMetadata)
				.withSSEAwsKeyManagementParams(sseAwsKeyManagementParams);
		amazonS3Client.putObject(putObjectRequest);
		
		fileInputStream.close();
	}
	
	//====================================================================================================
		
	public static void uploadMetadataWithKMS(String fileLocation, String bucketName, 
			String objectPrefix, Map<String, String> keyPairs, String kmsKeyId) 
					throws AmazonServiceException, SdkClientException, IOException {
		
		File inputFile = new File(fileLocation);
		FileInputStream fileInputStream = new FileInputStream(inputFile);
		ObjectMetadata objectMetadata = getObjectMetadata(fileInputStream);		
		SSEAwsKeyManagementParams sseAwsKeyManagementParams = new SSEAwsKeyManagementParams(kmsKeyId);

		for(Map.Entry<String, String> keyPair : keyPairs.entrySet()) {
			objectMetadata.addUserMetadata(keyPair.getKey(), keyPair.getValue());
		}
		
		PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, objectPrefix,
				fileInputStream, objectMetadata)
				.withSSEAwsKeyManagementParams(sseAwsKeyManagementParams);
		amazonS3Client.putObject(putObjectRequest);
		
		fileInputStream.close();
	}
	
	//====================================================================================================
	
	public static InventoryConfiguration getInventoryConfiguration(String bucketName, String inventoryId) 
			throws AmazonServiceException, SdkClientException {
		
		GetBucketInventoryConfigurationResult inventoryConfigurationResult = 
				amazonS3Client.getBucketInventoryConfiguration(bucketName, inventoryId);
		return inventoryConfigurationResult.getInventoryConfiguration();
	}
	
	//====================================================================================================
	
	public static void setInventoryConfigurationWithKMS(String accountId, String sourceBucketName, 
			String inventoryId, boolean isEnabled, String format, String frequency,
			String destinationBucketName, String destinationObjectPrefix, String kmsKeyId) 
					throws AmazonServiceException, SdkClientException {
		
		//create configuration request object values
		ServerSideEncryptionKMS sseKMS = new ServerSideEncryptionKMS().withKeyId(kmsKeyId);		
		InventoryS3BucketDestination s3BucketDestination = new InventoryS3BucketDestination()
				.withAccountId(accountId)
				.withBucketArn(destinationBucketName)
				.withPrefix(destinationObjectPrefix)
				.withFormat(format)
				.withEncryption(sseKMS);		
		InventoryDestination inventoryDestination = new InventoryDestination()
				.withS3BucketDestination(s3BucketDestination);
		InventorySchedule inventorySchedule = new InventorySchedule()
				.withFrequency(frequency);
		
		//create configuration request object
		InventoryConfiguration inventoryConfiguration = new InventoryConfiguration();
		inventoryConfiguration.setEnabled(isEnabled);
		inventoryConfiguration.setDestination(inventoryDestination);
		inventoryConfiguration.setSchedule(inventorySchedule);
		inventoryConfiguration.setId(inventoryId);
		
		//optional fields below  - might be best to implement as builder long term
		inventoryConfiguration.setIncludedObjectVersions(InventoryIncludedObjectVersions.Current);
		inventoryConfiguration.addOptionalField(InventoryOptionalField.LastModifiedDate);
		inventoryConfiguration.addOptionalField(InventoryOptionalField.StorageClass);
		inventoryConfiguration.addOptionalField(InventoryOptionalField.Size);
		
		SetBucketInventoryConfigurationRequest inventoryConfigurationRequest = 
				new SetBucketInventoryConfigurationRequest(sourceBucketName, inventoryConfiguration);
		amazonS3Client.setBucketInventoryConfiguration(inventoryConfigurationRequest);
	}
	
	//====================================================================================================
	
	public static void deleteS3Object(String bucketName, String objectPrefix) {
		DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(bucketName, objectPrefix);
		amazonS3Client.deleteObject(deleteObjectRequest);		
	}
	
	//====================================================================================================
	
	public static void deleteS3Bucket(String bucketName) {
		DeleteBucketRequest deleteBucketRequest = new DeleteBucketRequest(bucketName);
		amazonS3Client.deleteBucket(deleteBucketRequest);
	}
}