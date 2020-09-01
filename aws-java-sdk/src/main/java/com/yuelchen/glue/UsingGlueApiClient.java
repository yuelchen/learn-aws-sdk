package com.yuelchen.glue;

import java.util.List;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.Table;

/**
 * 
 * @author 	Yue Chen
 * @version	1.0.0
 * @since 	2019-09-01
 */
public class UsingGlueApiClient {
	
	/**
	 * The main method. 
	 * 
	 * @param args			an array of Strings as main method arguments.
	 */
	public static void main(String[] args) {
		
		//create use-case data
		String accountId = "123456789012";		
		String databaseName = "AmazonStore";
		String[] tableNames = {"Products", "Customers", "PurchaseHistory"};
		
		//create database
		GlueApiClient.createDatabase(accountId, databaseName, "My example clone of Amazon Store database.");
		
		//create tables under given database
		for(String tableName: tableNames) {
			GlueApiClient.createTable(accountId, databaseName, tableName);
		}
		
		//list all glue databases and print database name and it's tables
		List<Database> databases = GlueApiClient.getDatabasesList(accountId);
		System.out.println(String.format("There are '%d' databases in AWS account with Id '%s'", 
				databases.size(), accountId));
		for(Database database : databases) {
			List<Table> tables = GlueApiClient.getTablesList(accountId, database.getName());
			System.out.println(String.format("Found database named '%s' with description '%s' "
					+ "containing '%d' tables", database.getName(), database.getDescription(),
					tables.size()));			
		}
	}
}