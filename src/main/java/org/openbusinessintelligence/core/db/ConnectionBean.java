package org.openbusinessintelligence.core.db;

import static org.junit.Assert.fail;

import java.io.FileInputStream;
import java.sql.*;
import java.io.*;
import java.util.Properties;
import java.util.List;
import java.util.ArrayList;

import javax.naming.InitialContext;
import javax.sql.DataSource;

import org.openbusinessintelligence.core.file.*;
import org.slf4j.LoggerFactory;

public class ConnectionBean {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(ConnectionBean.class);
	
    // Declarations of bean properties
	//  properties
    private String propertyFile = "";
    private String databaseDriver = "";
    private String connectionURL = "";
    private String userName = "";
    private String passWord = "";
    private String dataSourceName = "";
    private String catalogName = "";
    private String schemaName = "";
    private String keyWords = "";
    private String quoteString = "";
    //
    private Connection connection = null;
    private DatabaseMetaData metadata = null;
    private String databaseProductName = null;
    
    // Constructor
    public ConnectionBean() {
        super();
    }

    // Setter methods
    public void setPropertyFile(String property) {
    	propertyFile = property;
    }
    
    public void setDataSourceName(String property) {
    	dataSourceName = property;
    }

    public void setDatabaseDriver(String property) {
        databaseDriver = property;
    }

    public void setConnectionURL(String property) {
        connectionURL = property;
    }

    public void setUserName(String property) {
        userName = property;
    }

    public void setPassWord(String property) {
        passWord = property;
    }

    public void setCatalogName(String property) {
        catalogName = property;
    }

    public void setSchemaName(String property) {
        schemaName = property;
    }

    // Getter methods
    public Connection getConnection() {
    	return connection;
    }
    
    public String getCatalogName() {
    	return catalogName;
    }
    
    public String getSchemaName() {
    	return schemaName;
    }

    public String getDatabaseProductName() {
    	return databaseProductName;
    }
    
    // Get complete identifier string
    public String getObjectIdentifier(String objectName) {
		logger.debug("Getting complete identifier for object " + objectName);
		
		String identifier = "";
		if (!(catalogName == null || catalogName.equals(""))) {
	    	if (keyWords.contains(catalogName.toUpperCase())) {
	    		identifier += quoteString + catalogName + quoteString + ".";
	    	}
	    	else {
	    		identifier += catalogName + ".";
	    	}
	    }
	    if (!(schemaName == null || schemaName.equals(""))) {
	    	if (keyWords.contains(schemaName.toUpperCase())) {
	    		identifier += quoteString + schemaName + quoteString + ".";
	    	}
	    	else {
	    		identifier += schemaName + ".";
	    	}
	    }
    	if (keyWords.contains(objectName.toUpperCase())) {
    		identifier += quoteString + objectName + quoteString + ".";
    	}
    	else {
    		identifier += objectName;
    	}
    	
    	return identifier;
    }
    
    // Get column string
    public String getColumnIdentifier(String columnName) throws Exception {
		logger.debug("Getting identifier for column " + columnName);
		boolean isNumber = false;
		try {
			Integer.parseInt(columnName);
			isNumber = true;
		}
		catch (Exception e) {
			isNumber = false;
		}

		String identifier = "";
		try {
	    	if (keyWords.contains(columnName.toUpperCase()) || isNumber) {
	    		identifier += quoteString + columnName.toUpperCase() + quoteString;
	    	}
	    	else {
	    		identifier += columnName;
	    	}
		}
		catch (Exception e) {
			logger.error(e.toString());
			throw e;
		}
    	
    	return identifier;
    }
    
    //Get value expression
    private String getValueExpression(Object property) {
    	String expression = property.toString();
    	return expression;
    }
    
    public String[] getTableList() throws Exception {
    	ResultSet dbTables = null;
    	int size = 0;

    	logger.info("########################################");
    	logger.debug("RDBMS type: " + databaseProductName);
    	logger.debug("Get tables for schema: " + schemaName);
    	try {
	    	if (databaseProductName.toUpperCase().contains("MYSQL")) {
	        	logger.debug("Serching in a jdbc catalog...");
		    	dbTables = metadata.getTables(schemaName, null, null, null);
		        logger.debug("Tables obtained");
	    	}
	    	else if (databaseProductName.toUpperCase().contains("ORACLE") || databaseProductName.toUpperCase().contains("DB2")) {
	        	logger.debug("Serching in a jdbc schema...");
	        	dbTables = metadata.getTables(null, schemaName.toUpperCase(), null, null);
		        logger.debug("Tables obtained");
	    	}
	    	else {
	    		dbTables = metadata.getTables(null, schemaName, null, null);
		        logger.debug("Tables obtained");
	    	}
    	}
		catch (Exception e) {
			logger.error("Exception: \n" + e.toString());
			throw e;
		}
    	String[] tableList = null;
    	try {
    		List<String> tableArray = new ArrayList<String>();
	    	while (dbTables.next()) {
	    		tableArray.add(dbTables.getString("TABLE_NAME"));
	    		logger.info(dbTables.getString("TABLE_SCHEM") + "." + dbTables.getString("TABLE_NAME"));
	    	}
	    	tableList = new String[tableArray.size()];
	    	int i = 0;
	    	for(String table : tableArray) {
	    		tableList[i] = table;
	    		i++;
	    	}
    	}
		catch (Exception e) {
			logger.error("Exception: \n" + e.toString());
			throw e;
		}    	
    	return tableList;
    }
    
    // Execution methods
    public void openConnection() throws Exception  {
    	
    	logger.info("Opening connection...");
    	
    	try {
	        if (dataSourceName == null ||dataSourceName.equals("")) {
	        	// Get connection using driver
	        	if (propertyFile == null || propertyFile.equals("")) {
	            	// Use given username and password
	            	logger.info("Using username & password");
		        	Class.forName(databaseDriver).newInstance();
		        	logger.info("Loaded database driver " + databaseDriver);
	            	connection = DriverManager.getConnection(connectionURL, userName, passWord);
	        	}
	        	else {
	            	// Use property file
	            	logger.info("Using property file " + propertyFile);
	            	Properties connectionProperties = new Properties();
	            	connectionProperties.load(new FileInputStream("datasources/" + propertyFile + ".properties"));
	        		if (databaseDriver == null || databaseDriver.equals("")) {
	        			// Get driver and url from property file
	        			databaseDriver = connectionProperties.getProperty("driver");
	        			connectionURL = connectionProperties.getProperty("url");
	        		}
		        	Class.forName(databaseDriver).newInstance();
	        		connection = DriverManager.getConnection(connectionURL, connectionProperties);
	        	}
	        	logger.debug("Connected to database " + connectionURL);
	        }
	        else {
	        	// Get connection from application server
	        	InitialContext ic = new InitialContext();
	        	DataSource ds = (DataSource)ic.lookup("java:comp/env/jdbc/" + dataSourceName.toLowerCase());
	        	connection = ds.getConnection();
	        	logger.debug("Connected to database " + dataSourceName);
	        }
	        
	    	logger.info("Opened connection");
	
	    	metadata = connection.getMetaData();
	    	databaseProductName = metadata.getDatabaseProductName();
	    	//FileInputBean keyWordFile = new FileInputBean();
	    	InputStream keyWordFile = Thread.currentThread().getContextClassLoader().getResourceAsStream("conf/SQL2003Keywords.txt");
	    	java.util.Scanner scanner = new java.util.Scanner(keyWordFile).useDelimiter("\\A");
	    	keyWords = scanner.next().replace("\n", ",");
	    	keyWordFile.close();
	    	scanner.close();
	    	keyWords += metadata.getSQLKeywords();
	    	logger.debug("Keywords: " + keyWords);
	    	quoteString = metadata.getIdentifierQuoteString();
	
	    	logger.info("########################################");
	    	logger.info("Found catalogs:");
	    	ResultSet dbCatalogs = metadata.getCatalogs();    	
	    	while (dbCatalogs.next()) {
	    		logger.info(dbCatalogs.getString("TABLE_CAT"));
	    	}
	
	    	logger.info("########################################");
	    	logger.info("Found schemas:");
	    	ResultSet dbSchemas = metadata.getSchemas();
	    	while (dbSchemas.next()) {
	    		logger.info(dbSchemas.getString("TABLE_SCHEM"));
	    	}
    	}
    	catch (Exception e) {
			logger.error(e.getMessage());
		    throw e;
    	}
    }
    
    public void closeConnection() throws Exception {
    	logger.info("Closing connection");
    	connection.close();
    	logger.info("Closed connection");
    }

}
