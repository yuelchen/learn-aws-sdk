package com.yuelchen.glue;

import com.amazonaws.ResponseMetadata;
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
	 * The Amazon Glue Client for making API requests. 
	 */
    public static AWSGlue amazonGlueClient = AWSGlueClientBuilder.defaultClient();

    //====================================================================================================
    
    /**
     * Returns the response metadata for given database creation request. 
     * 
     * @param accountId					the AWS account Id. 
     * @param databaseName				the name of the database to be created.
     * @param databaseDescription		the description of the database to be created - optional field.
     * 
     * @return							the response metadata object for given creation request. 
     * 
     * @throws AWSGlueException			thrown when there is an issue creating databases in Glue.
     */
    public static ResponseMetadata createDatabase(String accountId, String databaseName, 
    		String databaseDescription) throws AWSGlueException {
    	
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
     * @param accountId					the AWS account Id. 
     * @param databaseName				the database name for which table will be created under. 
     * @param tableName					the name of table to be created.
     * 
     * @return							the response metadata object for given creation request. 
     * 
     * @throws AWSGlueException			thrown when there is an issue creating table in Glue. 
     */
    public static ResponseMetadata createTable(String accountId, String databaseName, 
    		String tableName) throws AWSGlueException {
    	
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
     * @throws AWSGlueException			thrown when there is an issue retrieving databases from Glue.
     */
    public static List<Database> getDatabasesList(String accountId) 
    		throws AWSGlueException {
    	
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
     * @throws AWSGlueException			thrown when there is an issue retrieving database from Glue.
     */
    public static Database getDatabase(String accountId, String databaseName) 
    		throws AWSGlueException {
    	
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
     * @throws AWSGlueException			thrown when there is an issue retrieving list of tables from Glue. 
     */
    public static List<Table> getTablesList(String accountId, String databaseName) 
    		throws AWSGlueException {
    	
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
     * @throws AWSGlueException			thrown when there is an issue retrieving table from Glue. 
     */
    public static Table getTablesList(String accountId, String databaseName, String tableName) 
    		throws AWSGlueException {
    	
        GetTableRequest getTableRequest = new GetTableRequest();
        getTableRequest.setCatalogId(accountId);
        getTableRequest.setDatabaseName(databaseName);
        getTableRequest.setName(tableName);

        GetTableResult getTableResult = amazonGlueClient.getTable(getTableRequest);
        return getTableResult.getTable();
    }
}