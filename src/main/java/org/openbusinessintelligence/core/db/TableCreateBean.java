package org.openbusinessintelligence.core.db;

import java.io.FileInputStream;
import java.sql.*;
import java.util.Properties;

import javax.naming.InitialContext;
import javax.sql.DataSource;
import org.slf4j.LoggerFactory;

public class TableCreateBean {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(TableCreateBean.class);

    // Target properties
    private ConnectionBean targetCon = null;
    private String targetCatalog = "";
    private String targetSchema = "";
    private String targetTable = "";
    private String[] targetColumns = null;
    private String[] targetColumnDefinitions = null;
    
    // Options
    private boolean dropIfExists = false;
    
    // Constructor
    public TableCreateBean() {
        super();
    }
    
    // Set target properties methods
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
    
    public void setTargetConnection(ConnectionBean property) {
    	targetCon = property;
    }

    
    // Execution methods    
    public void createTable() throws Exception {
    	logger.info("########################################");
    	logger.info("CREATING TABLE");
    	
    	String currentCatalog;
    	String currentSchema;
    	String sqlText;
    	
    	boolean tableExistsFlag = false;
    	
    	DatabaseMetaData dbmd = targetCon.getConnection().getMetaData();
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
	    	targetStmt = targetCon.getConnection().prepareStatement(sqlText);
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
	    	targetStmt = targetCon.getConnection().prepareStatement(sqlText);
	    	targetStmt.executeUpdate();
	    	targetStmt.close();
	    
    	}
    	
        logger.info("TABLE CREATED");
        logger.info("########################################");
    	
    }
    
}