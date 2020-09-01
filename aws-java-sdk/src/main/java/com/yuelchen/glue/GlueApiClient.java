package com.yuelchen.glue;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.*;
import java.util.List;

/**
 * Amazon Glue API client handler for performing Glue operations. 
 * 
 * @author 	yuelchen
 * @version	1.0.0
 * @since 	2019-09-01
 */
public class GlueApiClient {
	
	/**
	 * The default Amazon Glue Client for making API requests. 
	 */
    public static AWSGlue amazonGlueClient = AWSGlueClientBuilder.defaultClient();

    //====================================================================================================
    
    /** 
     * Private constructor.
     */
    private GlueApiClient() {}
    
    //====================================================================================================
    
    /**
     * Returns the response metadata for given database creation request. 
     * 
     * Visit amazon documentation for DatabaseInput to explore additional information to be 
     * specified for when creating database.
     * https://docs.aws.amazon.com/glue/latest/webapi/API_DatabaseInput.html
     * 
     * @param accountId					the AWS account Id. 
     * @param databaseName				the name of the database to be created.
     * @param databaseDescription		the description of the database to be created - optional field.
     * 
     * @return							the response metadata object for given creation request. 
     * 
     * @throws AmazonServiceException	thrown when call was successfully sent but Glue could not process
     * 									the requested action.
     * @throws SdkClientException		thrown when call couldn't be reached or wasn't unknown to Glue. 
     */
    public static ResponseMetadata createDatabase(String accountId, String databaseName, 
    		String databaseDescription) throws AmazonServiceException, SdkClientException {
    	
    	DatabaseInput databaseInput = new DatabaseInput();
    	databaseInput.setName(databaseName);
    	databaseInput.setDescription(databaseDescription); 
    	
    	CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest();
    	createDatabaseRequest.setCatalogId(accountId);
    	createDatabaseRequest.setDatabaseInput(databaseInput);
    	
    	CreateDatabaseResult createDatabaseResult = amazonGlueClient.createDatabase(createDatabaseRequest);
    	return createDatabaseResult.getSdkResponseMetadata();
    }
    
    //====================================================================================================
    
    /**
     * Returns the response metadata for given table creation request. 
     * 
     * Visit amazon documentation for TableInput to explore additional information to be 
     * specified for when creating table. 
     * https://docs.aws.amazon.com/glue/latest/webapi/API_TableInput.html
     * 
     * @param accountId					the AWS account Id. 
     * @param databaseName				the database name for which table will be created under. 
     * @param tableName					the name of table to be created.
     * 
     * @return							the response metadata object for given creation request. 
     * 
     * @throws AmazonServiceException	thrown when call was successfully sent but Glue could not process
     * 									the requested action.
     * @throws SdkClientException		thrown when call couldn't be reached or wasn't unknown to Glue. 
     */
    public static ResponseMetadata createTable(String accountId, String databaseName, 
    		String tableName) throws AmazonServiceException, SdkClientException {
    	
    	TableInput tableInput = new TableInput();
    	tableInput.setName(tableName);
    	
    	CreateTableRequest createTableRequest = new CreateTableRequest();
    	createTableRequest.setCatalogId(accountId);
    	createTableRequest.setDatabaseName(databaseName);
    	createTableRequest.setTableInput(tableInput);
    	
    	CreateTableResult createTableResult = amazonGlueClient.createTable(createTableRequest);
    	return createTableResult.getSdkResponseMetadata();
    }
    
    //====================================================================================================
    
    /**
     * Returns a list of Glue Database objects if account Id exists. 
     * 
     * @param accountId					the AWS account Id. 
     * 
     * @return							a list of Glue Database objects.
     * 
     * @throws AmazonServiceException	thrown when call was successfully sent but Glue could not process
     * 									the requested action.
     * @throws SdkClientException		thrown when call couldn't be reached or wasn't unknown to Glue. 
     */
    public static List<Database> getDatabasesList(String accountId) 
    		throws AmazonServiceException, SdkClientException {
    	
        GetDatabasesRequest getDatabasesRequest = new GetDatabasesRequest();
        getDatabasesRequest.setCatalogId(accountId);

        GetDatabasesResult getDatabasesResult = amazonGlueClient.getDatabases(getDatabasesRequest);
        return getDatabasesResult.getDatabaseList();
    }
    
    //====================================================================================================

    /**
     * Returns a Glue Database object if account Id and database exists. 
     * 
     * @param accountId					the AWS account Id. 
     * @param databaseName				the database name in Glue. 
     * 
     * @return							a Glue Database object. 
     * 
     * @throws AmazonServiceException	thrown when call was successfully sent but Glue could not process
     * 									the requested action.
     * @throws SdkClientException		thrown when call couldn't be reached or wasn't unknown to Glue. 
     */
    public static Database getDatabase(String accountId, String databaseName) 
    		throws AmazonServiceException, SdkClientException {
    	
        GetDatabaseRequest getDatabaseRequest = new GetDatabaseRequest();
        getDatabaseRequest.setCatalogId(accountId);
        getDatabaseRequest.setName(databaseName);

        GetDatabaseResult getDatabaseResult = amazonGlueClient.getDatabase(getDatabaseRequest);
        return getDatabaseResult.getDatabase();
    }
    
    //====================================================================================================

    /**
     * Returns a list of Glue Table objects if account Id and database exists. 
     * 
     * @param accountId					the AWS account Id. 
     * @param databaseName				the database name in Glue. 
     * 
     * @return							a list of Glue Table objects.
     * 
     * @throws AmazonServiceException	thrown when call was successfully sent but Glue could not process
     * 									the requested action.
     * @throws SdkClientException		thrown when call couldn't be reached or wasn't unknown to Glue. 
     */
    public static List<Table> getTablesList(String accountId, String databaseName) 
    		throws AmazonServiceException, SdkClientException {
    	
        GetTablesRequest getTablesRequest = new GetTablesRequest();
        getTablesRequest.setCatalogId(accountId);
        getTablesRequest.setDatabaseName(databaseName);

        GetTablesResult getTablesResult = amazonGlueClient.getTables(getTablesRequest);
        return getTablesResult.getTableList();
    }
    
    //====================================================================================================

    /**
     * Return a Glue Table object if account Id, database and table exists.
     * 
     * @param accountId					the AWS account Id. 
     * @param databaseName				the database name in Glue. 
     * @param tableName					the table name under database name.
     * 
     * @return							a Glue Table object. 
     * 
     * @throws AmazonServiceException	thrown when call was successfully sent but Glue could not process
     * 									the requested action.
     * @throws SdkClientException		thrown when call couldn't be reached or wasn't unknown to Glue. 
     */
    public static Table getTablesList(String accountId, String databaseName, String tableName) 
    		throws AmazonServiceException, SdkClientException {
    	
        GetTableRequest getTableRequest = new GetTableRequest();
        getTableRequest.setCatalogId(accountId);
        getTableRequest.setDatabaseName(databaseName);
        getTableRequest.setName(tableName);

        GetTableResult getTableResult = amazonGlueClient.getTable(getTableRequest);
        return getTableResult.getTable();
    }
}