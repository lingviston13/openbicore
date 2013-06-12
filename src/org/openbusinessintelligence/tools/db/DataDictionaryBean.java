package org.openbusinessintelligence.tools.db;

import java.util.*;
import java.io.*;
import java.sql.*;

import javax.naming.*;
import javax.sql.*;
import javax.xml.parsers.*;

import org.slf4j.LoggerFactory;
import org.w3c.dom.*;

/**
 * Class for replication of database tables between databases
 * @author Nicola Marangoni
 */
public class DataDictionaryBean {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(DataDictionaryBean.class);

    // Declarations of bean properties
	// Source properties
    private String sourcePropertyFile = "";
    private String sourceDatabaseDriver = "";
    private String sourceConnectionURL = "";
    private String sourceUserName = "";
    private String sourcePassWord = "";
    private String sourceName = "";
    private String sourceTable = "";
    private String sourceQuery = "";

    // Target properties
    private String targetPropertyFile = "";
    private String targetDatabaseDriver = "";
    private String targetConnectionURL = "";
    private String targetUserName = "";
    private String targetPassWord = "";
    private String targetName = "";
    private String targetTable = "";
    private String targetColumns = "";
    
    // Mapping properties
    private String mappingDefFile = "";
    private String[] sourceMapColumns = null;
    private String[] targetMapColumns = null;
    private String[] targetDefaultColumns = null;
    private String[] targetDefaultValues = null;

    // Declarations of internally used variables
    private Properties sourceProperties = null;
    private Properties targetProperties = null;
    private Connection sourceCon = null;
    private Connection targetCon = null;
    //
    private String[] sourceColumnNames = null;
    private String[] sourceColumnType = null;
    private int[] sourceColumnLength = null;
    private int[] sourceColumnPrecision = null;
    private int[] sourceColumnScale = null;
    private String[] sourceColumnDefinitions = null;
    //
    private String[] targetColumnNames = null;
    private String[] targetColumnType = null;
    private int[] targetColumnLength = null;
    private int[] targetColumnPrecision = null;
    private int[] targetColumnScale = null;
    private String[] targetColumnDefinitions = null;
    
    private int[] columnPkPositions = null;
    
    // Constructor
    public DataDictionaryBean() {
        super();
    }
    

    // Set source properties methods
    public void setSourcePropertyFile(String property) {
    	sourcePropertyFile = property;
    }
    
    public void setSourceName(String property) {
        sourceName = property;
    }

    public void setSourceDatabaseDriver(String property) {
        sourceDatabaseDriver = property;
    }

    public void setSourceConnectionURL(String property) {
        sourceConnectionURL = property;
    }

    public void setSourceUserName(String property) {
        sourceUserName = property;
    }

    public void setSourcePassWord(String property) {
        sourcePassWord = property;
    }

    public void setSourceTable(String property) {
        sourceTable = property;
    }

    public void setSourceQuery(String property) {
        sourceQuery = property;
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

    public void setTargetTable(String property) {
        targetTable = property;
    }

    public void setTargetColumns(String property) {
        targetColumns = property;
    }
    

    // Set optional mapping properties 
    public void setMappingDefFile(String property) {
    	mappingDefFile = property;
    }
    
    public void setSourceMapColumns(String[] property) {
    	sourceMapColumns = property;
    }
    
    public void setTargetMapColumns(String[] property) {
    	targetMapColumns = property;
    }
    
    public void setTargetDefaultColumns(String[] property) {
    	targetDefaultColumns = property;
    }
    
    
    // Get methods    
    public String[] getSourceColumnNames() {
    	return sourceColumnNames;
    }
    
    public String[] getSourceColumnDefinitions() {
    	return sourceColumnDefinitions;
    }
    
    public String[] getTargetColumnNames() {
    	return targetColumnNames;
    }
    
    public String[] getTargetColumnDefinitions() {
    	return targetColumnDefinitions;
    }
    
    public int[] getSourceColumnPkPositions() {
    	return columnPkPositions;
    }
    
    
    // Execution methods
    private void openSourceConnection() throws Exception  {
   	
    	DataSource ds = null;
    	
    	logger.info("Opening source connection...");
    	
        if (sourceName == null ||sourceName.equals("")) {
        	Class.forName(sourceDatabaseDriver).newInstance();
        	logger.info("Loaded database driver " + sourceDatabaseDriver);
        	logger.info("URL: " + sourceConnectionURL);
        	if (sourcePropertyFile == null || sourcePropertyFile.equals("")) {
            	
            	logger.info("Using username & password");
            	sourceCon = DriverManager.getConnection(sourceConnectionURL, sourceUserName, sourcePassWord);
        	}
        	else {
            	
            	logger.info("Using property file " + sourcePropertyFile);
            	sourceProperties = new Properties();
        		sourceProperties.load(new FileInputStream(sourcePropertyFile));
        		sourceCon = DriverManager.getConnection(sourceConnectionURL, sourceProperties);
        	}
        	logger.debug("Connected to database " + sourceConnectionURL);
        }
        else {
        	InitialContext ic;
        	ic = new InitialContext();
        	ds = (DataSource)ic.lookup("java:comp/env/jdbc/" + sourceName.toLowerCase());
        	sourceCon = ds.getConnection();
        	logger.debug("Connected to database " + sourceName);
        }
        
    	logger.info("Opened source connection");
    
    }
    
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
        	logger.debug("Connected to database " + sourceName);
        }
        
    	logger.info("Opened target connection");
    	
    }
    
    // Main methods    
    public void retrieveColumns() throws Exception {

    	String sourceProductName;
    	String targetProductName;
    	
    	String sqlText;
       	if (sourceQuery == null || sourceQuery.equals("")) {
       		sqlText = "SELECT * FROM " + sourceTable;
       	}
       	else {
       		sqlText = sourceQuery;
       	}
    	
       	logger.info("SQL: " + sqlText + ": getting columns...");
        
       	openSourceConnection();
       	sourceProductName = sourceCon.getMetaData().getDatabaseProductName();
        PreparedStatement columnStmt = sourceCon.prepareStatement(sqlText);
        ResultSet rs = columnStmt.executeQuery();
        ResultSetMetaData rsmd = rs.getMetaData();

        sourceColumnNames = new String[rsmd.getColumnCount()];
        sourceColumnType = new String[rsmd.getColumnCount()];
        sourceColumnLength = new int[rsmd.getColumnCount()];
        sourceColumnPrecision = new int[rsmd.getColumnCount()];
        sourceColumnScale = new int[rsmd.getColumnCount()];
        sourceColumnDefinitions = null;
        sourceColumnDefinitions = new String[rsmd.getColumnCount()];
        //
        targetColumnNames = new String[rsmd.getColumnCount()];
        targetColumnType = new String[rsmd.getColumnCount()];
        targetColumnLength = new int[rsmd.getColumnCount()];
        targetColumnPrecision = new int[rsmd.getColumnCount()];
        targetColumnScale = new int[rsmd.getColumnCount()];
        targetColumnDefinitions = new String[rsmd.getColumnCount()];
        columnPkPositions = new int[rsmd.getColumnCount()];
        
        openTargetConnection();
        targetProductName = targetCon.getMetaData().getDatabaseProductName();
        
        logger.info("Source RDBMS product: " + sourceProductName);
        logger.info("Targer RDBMS product: " + targetProductName);
        
       	for (int i = 1; i <= rsmd.getColumnCount(); i++) {
        	sourceColumnNames[i - 1] = rsmd.getColumnName(i).toUpperCase();
        	targetColumnNames[i - 1] = sourceColumnNames[i - 1];
        	if (sourceMapColumns != null) {
	        	for (int mc = 0; mc < sourceMapColumns.length; mc++) {
	        		if (sourceMapColumns[mc].equalsIgnoreCase(sourceColumnNames[i - 1])) {
	        			sourceColumnNames[i - 1] = targetMapColumns[mc];
	        			targetColumnNames[i - 1] = targetMapColumns[mc];
	        		}
	        	}
        	}

        	//*******************************
        	// set source column properties
        	sourceColumnType[i - 1] = rsmd.getColumnTypeName(i).toUpperCase();
        	sourceColumnLength[i - 1] = rsmd.getColumnDisplaySize(i);
        	sourceColumnPrecision[i - 1] = rsmd.getPrecision(i);
        	sourceColumnScale[i - 1] = rsmd.getScale(i);
        	sourceColumnDefinitions[i - 1] = sourceColumnType[i - 1];
        	// Column definition
        	if (sourceColumnPrecision[i - 1] > 0) {
        		sourceColumnDefinitions[i - 1] += "(" + sourceColumnPrecision[i - 1] + "," + sourceColumnScale[i - 1] + ")";
        	}
        	else if (sourceColumnLength[i - 1] > 0) {
        		sourceColumnDefinitions[i - 1] += "(" + sourceColumnLength[i - 1] + ")";
        	}

        	//*******************************
        	// set target column properties
        	if (sourceColumnType[i - 1].contains("CHAR") && sourceColumnLength[i - 1] == 1) {
           		targetColumnType[i - 1] = "CHAR (1)";
           	}
        	else if (sourceColumnType[i - 1].contains("CHAR") && sourceColumnLength[i - 1] > 1) {
        		if (targetProductName.toUpperCase().contains("ORACLE")) {
            		if (sourceColumnLength[i - 1] > 4000) {
            			targetColumnType[i - 1] = "CLOB";
            		}
            		else {
            			targetColumnType[i - 1] = "VARCHAR2";
            			targetColumnLength[i - 1] = sourceColumnLength[i - 1];
            		}
        		}
	        	else {
        			targetColumnType[i - 1] = "VARCHAR";
        			if (targetProductName.toUpperCase().contains("MICROSOFT") && (sourceColumnLength[i - 1] > 8000)) {
        				targetColumnLength[i - 1] = -1;
        			}
        			else {
        				targetColumnLength[i - 1] = sourceColumnLength[i - 1];
        			}
	        	}
           	}
           	else if (
           			sourceColumnType[i - 1].contains("DATE") ||
           			sourceColumnType[i - 1].contains("TIME")
           	) {
           		if (targetProductName.toUpperCase().contains("ORACLE")) {
               		targetColumnType[i - 1] = "DATE";
           		}
           		else {
           			targetColumnType[i - 1] = "DATETIME";
           		}
           	}
           	else if (
           			sourceColumnType[i - 1].contains("NUM") ||
           			sourceColumnType[i - 1].contains("BIN") ||
           			sourceColumnType[i - 1].contains("DEC") ||
           			sourceColumnType[i - 1].contains("INT") ||
           			sourceColumnType[i - 1].contains("DOU") ||
           			sourceColumnType[i - 1].contains("FLO")
           	) {
           		if (sourceColumnPrecision[i - 1] <= sourceColumnScale[i - 1]) {
           			targetColumnType[i - 1] = "FLOAT";
           		}
           		else {
           			if (targetProductName.toUpperCase().contains("ORACLE")) {
           				targetColumnType[i - 1] = "NUMBER";           				
           			}
           			else {
           				targetColumnType[i - 1] = "NUMERIC";
           			}
           		}    		
           	}
        	// Column definition
        	targetColumnDefinitions[i - 1] = targetColumnType[i - 1];
        	if (targetColumnPrecision[i - 1] > 0) {
        		targetColumnDefinitions[i - 1] += "(" + targetColumnPrecision[i - 1] + "," +targetColumnScale[i - 1] + ")";
        	}
        	else if (targetColumnLength[i - 1] != 0) {
        		logger.info(targetColumnNames[i - 1] + " " +  String.valueOf(targetColumnLength[i - 1]));
        		if (targetColumnLength[i - 1]==-1) {
        			targetColumnDefinitions[i - 1] += "(max)";
        		}
        		else {
        			targetColumnDefinitions[i - 1] += "(" + targetColumnLength[i - 1] + ")";
        		}
        	}
        	
           	logger.info("FOUND COLUMN Position: " + (i) + "  Name: " + sourceColumnNames[i - 1] + "  Definition: " + targetColumnDefinitions[i - 1]);
       	}
        rs.close();
        columnStmt.close();
    	logger.info(String.valueOf(sourceTable.split("\\.").length));
        String schema = null;
        if (sourceTable.split("\\.").length==2) {
        	schema = sourceTable.split("\\.")[0];
        	logger.info(schema);
        }
        ResultSet rspk = sourceCon.getMetaData().getPrimaryKeys(schema, schema, sourceTable.split("\\.")[sourceTable.split("\\.").length-1]);
        while (rspk.next()) {
        	logger.info("PRIMARY KEY Position: " + rspk.getObject("KEY_SEQ") + " Column: " + rspk.getObject("COLUMN_NAME"));
         	for (int i = 0; i < sourceColumnNames.length; i++) {
         		if (sourceColumnNames[i].equalsIgnoreCase(rspk.getString("COLUMN_NAME"))) {
         			columnPkPositions[i] = rspk.getInt("KEY_SEQ");
         		}
           	}
        }
        rspk.close();
	    sourceCon.close();

       	logger.info("SQL: " + sqlText + ": got columns");
    }
    
    public void retrieveMappingDefinition() throws Exception {
    	
    	// Load mapping definition file
    	logger.info("LOADING MAP DEFINITION FILE " + mappingDefFile + "...");
    	
    	org.w3c.dom.Document mappingXML = null;
    	
		DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
		javax.xml.parsers.DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
		mappingXML = docBuilder.parse(mappingDefFile);
		mappingXML.getDocumentElement().normalize();
		
		// Local variables
		NodeList nList;
		Node nNode;
		Element eElement;
		
		// get source to target column mapping
		nList = mappingXML.getElementsByTagName("columnMapping");
		sourceMapColumns = new String[nList.getLength()];
		targetMapColumns = new String[nList.getLength()];
		for (int i = 0; i < nList.getLength(); i++) {
 			nNode = nList.item(i);
 			if (nNode.getNodeType() == Node.ELEMENT_NODE) {
 				eElement = (Element)nNode;
 				sourceMapColumns[i] = eElement.getElementsByTagName("source").item(0).getChildNodes().item(0).getNodeValue();
 				targetMapColumns[i] = eElement.getElementsByTagName("target").item(0).getChildNodes().item(0).getNodeValue();
 			}
		}
		
		// get default value to target column mapping
		nList = mappingXML.getElementsByTagName("defaultValue");
		targetDefaultColumns = new String[nList.getLength()];
		targetDefaultValues = new String[nList.getLength()];
		for (int i = 0; i < nList.getLength(); i++) {
 			nNode = nList.item(i);
 			if (nNode.getNodeType() == Node.ELEMENT_NODE) {
 				eElement = (Element)nNode;
 				targetDefaultColumns[i] = eElement.getElementsByTagName("column").item(0).getChildNodes().item(0).getNodeValue();
 				targetDefaultValues[i] = eElement.getElementsByTagName("value").item(0).getChildNodes().item(0).getNodeValue();
 			}
		}
    	logger.info("LOADED MAP DEFINITION FILE");
    }
}
