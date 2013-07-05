package org.openbusinessintelligence.core.db;

import java.io.FileInputStream;
import java.sql.*;
import java.util.Properties;

import javax.naming.InitialContext;
import javax.sql.DataSource;

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
    private String tableName = "";
    private String queryText = "";
    private String[] queryParameters = null;
    //
    private Connection connection = null;
    private String databaseProductName = null;
    private String[] catalogs = null;
    private String[] schemas = null;
    
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

    public void setTableName(String property) {
        tableName = property;
    }

    public void setQueryText(String sq) {
        queryText = sq;
    }

    public void setQueryParameters(String[] qp) {
        queryParameters = qp;
    }

    // Getter methods
    public Connection getConnection() {
    	return connection;
    }

    public String getDatabaseProductName() {
    	return databaseProductName;
    }
    
    // Execution methods
    public void openConnection() throws Exception  {
    	
    	logger.info("Opening connection...");
    	
        if (dataSourceName == null ||dataSourceName.equals("")) {
        	Class.forName(databaseDriver).newInstance();
        	logger.info("Loaded database driver " + databaseDriver);
        	if (propertyFile == null || propertyFile.equals("")) {
            	
            	logger.info("Using username & password");
        		
            	connection = DriverManager.getConnection(connectionURL, userName, passWord);
        	}
        	else {
            	
            	logger.info("Using property file " + propertyFile);
        		
            	Properties connectionProperties = new Properties();
            	connectionProperties.load(new FileInputStream(propertyFile));
        		connection = DriverManager.getConnection(connectionURL, connectionProperties);
        	}
        	logger.debug("Connected to database " + connectionURL);
        }
        else {
        	InitialContext ic = new InitialContext();
        	DataSource ds = (DataSource)ic.lookup("java:comp/env/jdbc/" + dataSourceName.toLowerCase());
        	connection = ds.getConnection();
        	logger.debug("Connected to database " + dataSourceName);
        }
        
    	logger.info("Opened connection");

    	DatabaseMetaData metadata = connection.getMetaData();
    	databaseProductName = metadata.getDatabaseProductName();

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
    	logger.info("########################################");
    	logger.info("Found tables:");
    	ResultSet dbTables = metadata.getTables(null, null, null, null);
    	while (dbTables.next()) {
    		logger.info(dbTables.getString("TABLE_SCHEM") + "." + dbTables.getString("TABLE_NAME"));
    	}
    }
    
    public void closeConnection() throws Exception {
    	logger.info("Closing connection");
    	connection.close();
    	logger.info("Closed connection");
    }

}
