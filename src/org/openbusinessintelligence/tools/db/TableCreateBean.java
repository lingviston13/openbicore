package org.openbusinessintelligence.tools.db;

import java.io.FileInputStream;
import java.sql.*;
import java.util.Properties;

import javax.naming.InitialContext;
import javax.sql.DataSource;
import org.slf4j.LoggerFactory;

public class TableCreateBean {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(TableCreateBean.class);

    // Target properties
    private String targetPropertyFile = "";
    private String targetDatabaseDriver = "";
    private String targetConnectionURL = "";
    private String targetUserName = "";
    private String targetPassWord = "";
    private String targetName = "";
    private String targetCatalog = "";
    private String targetSchema = "";
    private String targetTable = "";
    private String[] targetColumns = null;
    private String[] targetColumnDefinitions = null;
    
    // Options
    private boolean dropIfExists = false;

    // Declarations of internally used variables
    private Properties targetProperties = null;
    private Connection targetCon = null;
    
    // Constructor
    public TableCreateBean() {
        super();
    }
    
    // Set target properties methods
    public void setTargetPropertyFile(String property) {
    	targetPropertyFile = property;
    }
    
    public void setTargetName(String property) {
        targetName = property;
    }

    public void setTargetDatabaseDriver(String property) {
        targetDatabaseDriver = property;
    }

    public void setTargetConnectionURL(String property) {
        targetConnectionURL = property;
    }

    public void setTargetUserName(String property) {
        targetUserName = property;
    }

    public void setTargetPassWord(String property) {
        targetPassWord = property;
    }

    public void setTargetCatalog(String property) {
        targetCatalog = property;
    }

    public void setTargetSchema(String property) {
        targetSchema = property;
    }

    public void setTargetTable(String property) {
        targetTable = property;
    }

    public void setTargetColumns(String[] property) {
    	targetColumns = property;
    }

    public void setTargetColumnDefinitions(String[] property) {
    	targetColumnDefinitions = property;
    }

    
    // Execution methods    
    private void openTargetConnection() throws Exception {
    	
    	DataSource ds = null;
    	
    	logger.info("Opening target connection");
    	
        if (targetName == null || targetName.equals("")) {
        	Class.forName(targetDatabaseDriver).newInstance();
        	logger.debug("Loaded database driver " + targetDatabaseDriver);
        	if (targetPropertyFile == null || targetPropertyFile.equals("")) {
               	targetCon = DriverManager.getConnection(targetConnectionURL, targetUserName, targetPassWord);
        	}
        	else {
        		targetProperties = new Properties();
        		targetProperties.load(new FileInputStream(targetPropertyFile));
               	targetCon = DriverManager.getConnection(targetConnectionURL, targetProperties);
        	}
        	
         	logger.debug("Connected to database " + targetConnectionURL);
        }
        else {
        	InitialContext ic;
        	ic = new InitialContext();
        	ds = (DataSource)ic.lookup("java:comp/env/jdbc/" + targetName.toLowerCase());
        	
        	targetCon = ds.getConnection();
        	logger.debug("Connected to database " + targetName);
        }
        
    	logger.info("Opened target connection");
    	
    }
    
    public void createTable() throws Exception {
    	logger.info("########################################");
    	logger.info("CREATING TABLE");
    	
    	String currentCatalog;
    	String currentSchema;
    	String sqlText;
    	
    	boolean tableExistsFlag = false;
    	
    	openTargetConnection();
    	DatabaseMetaData dbmd = targetCon.getMetaData();
    	//currentCatalog = targetCon.getCatalog();
    	//currentSchema = targetCon.getSchema();
    	ResultSet tables = dbmd.getTables(null, null, targetTable, null);
    	while(tables.next()) {
    		if (tables.getString(3).equals(targetTable)) {
    			tableExistsFlag = true;
    		}
    	}
    	
    	if ((dropIfExists == true ) && (tableExistsFlag == true)) {
    	
    		// Drop existing table
	       	sqlText = "DROP TABLE " + targetTable;
	
	       	// Execute prepared statement
	        PreparedStatement targetStmt;
	    	targetStmt = targetCon.prepareStatement(sqlText);
	    	targetStmt.executeUpdate();
	    	targetStmt.close();
    		
    	}
    	else if (tableExistsFlag == false) {
    	
    		// create table    	
	       	sqlText = "CREATE TABLE " + targetTable + "(";
	       	for (int i = 0; i < targetColumnDefinitions.length; i++) {
		    	if (i > 0) {
		    		sqlText += ",";
		    	}
	       		sqlText += targetColumns[i] + " " + targetColumnDefinitions[i];
	       	}
	       	sqlText += ")";
	
	       	// Execute prepared statement
	        PreparedStatement targetStmt;
	        logger.info("Creation statement:\n" + sqlText);
	    	targetStmt = targetCon.prepareStatement(sqlText);
	    	targetStmt.executeUpdate();
	    	targetStmt.close();
	    
    	}
    	
        targetCon.commit();
	    targetCon.close();
    	
        logger.info("TABLE CREATED");
        logger.info("########################################");
    	
    }
    
}