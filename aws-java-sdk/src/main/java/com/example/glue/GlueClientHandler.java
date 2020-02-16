package com.example.glue;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.*;
import java.util.List;

public class GlueClientHandler {
    public static AWSGlue client = AWSGlueClientBuilder.defaultClient();

    public static List<Database> getDatabasesList(String accountId) throws AWSGlueException {
        GetDatabasesRequest getDatabasesRequest = new GetDatabasesRequest();
        getDatabasesRequest.setCatalogId(accountId);

        GetDatabasesResult getDatabasesResult = client.getDatabases(getDatabasesRequest);
        return getDatabasesResult.getDatabaseList();
    }

    public static Database getDatabase(String accountId, String databaseName) throws AWSGlueException {
        GetDatabaseRequest getDatabaseRequest = new GetDatabaseRequest();
        getDatabaseRequest.setCatalogId(accountId);
        getDatabaseRequest.setName(databaseName);

        GetDatabaseResult getDatabaseResult = client.getDatabase(getDatabaseRequest);
        return getDatabaseResult.getDatabase();
    }

    public static List<Table> getTablesList(String accountId, String databaseName) throws AWSGlueException {
        GetTablesRequest getTablesRequest = new GetTablesRequest();
        getTablesRequest.setCatalogId(accountId);
        getTablesRequest.setDatabaseName(databaseName);

        GetTablesResult getTablesResult = client.getTables(getTablesRequest);
        return getTablesResult.getTableList();
    }

    public static Table getTablesList(String accountId, String databaseName, String tableName) throws AWSGlueException {
        GetTableRequest getTableRequest = new GetTableRequest();
        getTableRequest.setCatalogId(accountId);
        getTableRequest.setDatabaseName(databaseName);
        getTableRequest.setName(tableName);

        GetTableResult getTableResult = client.getTable(getTableRequest);
        return getTableResult.getTable();
    }
}