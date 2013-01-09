package org.openbusinessintelligence.tools.db;

import java.util.*;
import java.io.*;
import java.sql.*;

import javax.naming.*;
import javax.sql.*;
import javax.xml.parsers.*;

import org.w3c.dom.*;

/**
 * Class for replication of database tables between databases
 * @author Nicola Marangoni
 */
public class DataDictionaryBean {

	private final static java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(DataDictionaryBean.class.getPackage().getName());

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
    private String[] sourceColumnNames = null;
    private String[] sourceColumnDefinitions = null;
    private String[] sourceColumnOriginalDefinitions = null;
    private int[] sourceColumnPkPositions = null;
    
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
    
    public String[] getSourceColumnOriginalDefinitions() {
    	return sourceColumnOriginalDefinitions;
    }
    
    public int[] getSourceColumnPkPositions() {
    	return sourceColumnPkPositions;
    }
    
    
    // Execution methods
    private void openSourceConnection() throws Exception  {
   	
    	DataSource ds = null;
    	
    	LOGGER.info("Opening source connection...");
    	
        if (sourceName == null ||sourceName.equals("")) {
        	Class.forName(sourceDatabaseDriver).newInstance();
        	LOGGER.info("Loaded database driver " + sourceDatabaseDriver);
        	LOGGER.info("URL: " + sourceConnectionURL);
        	if (sourcePropertyFile == null || sourcePropertyFile.equals("")) {
            	
            	LOGGER.info("Using username & password");
            	sourceCon = DriverManager.getConnection(sourceConnectionURL, sourceUserName, sourcePassWord);
        	}
        	else {
            	
            	LOGGER.info("Using property file " + sourcePropertyFile);
            	sourceProperties = new Properties();
        		sourceProperties.load(new FileInputStream(sourcePropertyFile));
        		sourceCon = DriverManager.getConnection(sourceConnectionURL, sourceProperties);
        	}
        	LOGGER.fine("Connected to database " + sourceConnectionURL);
        }
        else {
        	InitialContext ic;
        	ic = new InitialContext();
        	ds = (DataSource)ic.lookup("java:comp/env/jdbc/" + sourceName.toLowerCase());
        	sourceCon = ds.getConnection();
        	LOGGER.fine("Connected to database " + sourceName);
        }
        
    	LOGGER.info("Opened source connection");
    
    }
    
    private void openTargetConnection() throws Exception {
    	
    	DataSource ds = null;
    	
    	LOGGER.info("Opening target connection");
    	
        if (targetName == null || targetName.equals("")) {
        	Class.forName(targetDatabaseDriver).newInstance();
        	LOGGER.fine("Loaded database driver " + targetDatabaseDriver);
        	if (targetPropertyFile == null || targetPropertyFile.equals("")) {
               	targetCon = DriverManager.getConnection(targetConnectionURL, targetUserName, targetPassWord);
        	}
        	else {
        		targetProperties = new Properties();
        		targetProperties.load(new FileInputStream(targetPropertyFile));
               	targetCon = DriverManager.getConnection(targetConnectionURL, targetProperties);
        	}
        	
         	LOGGER.fine("Connected to database " + targetConnectionURL);
        }
        else {
        	InitialContext ic;
        	ic = new InitialContext();
        	ds = (DataSource)ic.lookup("java:comp/env/jdbc/" + targetName.toLowerCase());
        	
        	targetCon = ds.getConnection();
        	LOGGER.fine("Connected to database " + sourceName);
        }
        
    	LOGGER.info("Opened target connection");
    	
    }
    
    // Column definition methods
    public String defineColumn(
    		String dbProduct,
    		String colType,
    		int colSize,
    		int colPrecision,
    		int colScale
    ) {
    	
    	String colDefinition = "";
    	
    	if (
    			dbProduct.equalsIgnoreCase("Informix Dynamic Server") &&
    			(
    					colType.toUpperCase().contains("INT") ||
    					colType.toUpperCase().contains("SERIAL")
    			)
    	) {
    		colDefinition = "NUMBER (" + (colSize - 1) + ")";
    	}
    	else if (colType.toUpperCase().contains("CHAR") && colSize > 1) {
    		if (colSize > 4000) {
    			colSize = 4000;
    		}
       		colDefinition = "VARCHAR2 (" + colSize + ")";
       	}
       	else if (colType.toUpperCase().contains("CHAR") && colSize == 1) {
       		colDefinition = "CHAR (1)";
       	}
       	else if (
       			colType.toUpperCase().contains("NUM") ||
       			colType.toUpperCase().contains("BIN") ||
       			colType.toUpperCase().contains("DEC") ||
       			colType.toUpperCase().contains("INT") ||
       			colType.toUpperCase().contains("DOU") ||
       			colType.toUpperCase().contains("FLO")
       	) {
       		if (colPrecision<=colScale) {
       			colDefinition = "FLOAT";
       		}
       		else {
           		colDefinition = "NUMBER (" + colPrecision + "," + colScale + ")";
       			
       		}    		
       	}
       	else if (
       			colType.toUpperCase().contains("DATE") ||
       			colType.toUpperCase().contains("TIME")
       	) {
       		colDefinition = "DATE";      		
       	}
       	else {
       		colDefinition = colType.toUpperCase();
       	}

       	return colDefinition;
    }
    
    // Get definition string for column
    public String defineOriginalColumn(
    		String colType,
    		int colSize,
    		int colPrecision,
    		int colScale
    ) {
    	
    	String colDefinition = colType.toUpperCase();
    	
    	if (colPrecision > 0) {
    		colDefinition += "(" + colPrecision + "," + colScale + ")";
    	}
    	else if (colSize > 0) {
    		colDefinition += "(" + colSize + ")";
    	}

       	return colDefinition;
    }
    
    // Main methods    
    public void retrieveColumns() throws Exception {

    	String sqlText;
       	if (sourceQuery == null || sourceQuery.equals("")) {
       		sqlText = "SELECT * FROM " + sourceTable;
       	}
       	else {
       		sqlText = sourceQuery;
       	}
    	
       	LOGGER.info("SQL: " + sqlText + ": getting columns...");
        
       	openSourceConnection();
        PreparedStatement columnStmt = sourceCon.prepareStatement(sqlText);
        ResultSet rs = columnStmt.executeQuery();
        ResultSetMetaData rsmd = rs.getMetaData();

        sourceColumnNames = new String[rsmd.getColumnCount()];
        sourceColumnDefinitions = new String[rsmd.getColumnCount()];
        sourceColumnOriginalDefinitions = new String[rsmd.getColumnCount()];
        sourceColumnPkPositions = new int[rsmd.getColumnCount()];
        
       	for (int i = 1; i <= rsmd.getColumnCount(); i++) {
        	sourceColumnNames[i - 1] = rsmd.getColumnName(i).toUpperCase();
        	if (sourceMapColumns != null) {
	        	for (int mc = 0; mc < sourceMapColumns.length; mc++) {
	        		if (sourceMapColumns[mc].equalsIgnoreCase(sourceColumnNames[i - 1])) {
	        			sourceColumnNames[i - 1] = targetMapColumns[mc];
	        		}
	        	}
        	}
           	sourceColumnDefinitions[i - 1] = defineColumn(
           		sourceCon.getMetaData().getDatabaseProductName(),
           		rsmd.getColumnTypeName(i),
           		rsmd.getColumnDisplaySize(i),
           		rsmd.getPrecision(i),
           		rsmd.getScale(i)
           	);
           	sourceColumnOriginalDefinitions[i - 1] = defineOriginalColumn(
               		rsmd.getColumnTypeName(i),
               		rsmd.getColumnDisplaySize(i),
               		rsmd.getPrecision(i),
               		rsmd.getScale(i)
            );
           	LOGGER.info("FOUND COLUMN Position: " + (i) + "  Name: " + sourceColumnNames[i - 1] + "  Definition: " + sourceColumnDefinitions[i - 1]);
       	}
        rs.close();
        columnStmt.close();
        ResultSet rspk = sourceCon.getMetaData().getPrimaryKeys(null, null, sourceTable.split("\\.")[sourceTable.split("\\.").length-1]);
        while (rspk.next()) {
        	LOGGER.info("PRIMARY KEY Position: " + rspk.getObject("KEY_SEQ") + " Column: " + rspk.getObject("COLUMN_NAME"));
         	for (int i = 0; i < sourceColumnNames.length; i++) {
         		if (sourceColumnNames[i].equalsIgnoreCase(rspk.getString("COLUMN_NAME"))) {
         			sourceColumnPkPositions[i] = rspk.getInt("KEY_SEQ");
         		}
           	}
        }
        rspk.close();
	    sourceCon.close();

       	LOGGER.info("SQL: " + sqlText + ": got columns");
    }
    
    public void retrieveMappingDefinition() throws Exception {
    	
    	// Load mapping definition file
    	LOGGER.info("LOADING MAP DEFINITION FILE " + mappingDefFile + "...");
    	
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
    	LOGGER.info("LOADED MAP DEFINITION FILE");
    }

    // Insert column definition in the given table
    public void executeInsert() throws Exception {
        LOGGER.info("########################################");
    	LOGGER.info("INSERTING DATA...");

    	String insertText;
    	openTargetConnection();    	
        PreparedStatement targetStmt;
        insertText = "DELETE " + targetTable;
	    LOGGER.info(insertText);
       	targetStmt = targetCon.prepareStatement(insertText);
        targetStmt.executeUpdate();
        targetStmt.close();
	    LOGGER.info("Rows deleted");
        insertText = "INSERT /*+APPEND*/ INTO " + targetTable +"(" + targetColumns + ") VALUES (?,?,?,?)";
	    
	    LOGGER.info(insertText);
	    LOGGER.fine("Statement prepared");
	    
	    int rowCount = 0;
	    
	    for (int i=0; i<sourceColumnNames.length; i++) {
	    	targetStmt = targetCon.prepareStatement(insertText);
	    	targetStmt.setObject(1, i+1);
	    	targetStmt.setObject(2, sourceColumnNames[i]);
	    	targetStmt.setObject(3, sourceColumnDefinitions[i]);
	    	if (sourceColumnPkPositions[i]>0) {
		    	targetStmt.setObject(4, sourceColumnPkPositions[i]);
	    	}
	    	else {
	    		targetStmt.setObject(4, null);
	    	}
		    targetStmt.executeUpdate();
		    targetStmt.close();
		    rowCount++;
	    }
        targetCon.commit();
	    targetCon.close();

	    LOGGER.info(rowCount + " rows totally inserted");
	    LOGGER.info(rowCount + " INSERT COMPLETED");
	    LOGGER.info("########################################");
    }
}
