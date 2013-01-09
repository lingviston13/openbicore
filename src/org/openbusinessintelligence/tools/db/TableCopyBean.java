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
public class TableCopyBean {

	private final static java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(TableCopyBean.class.getPackage().getName());

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
    private String[] queryParameters = null;

    // Target properties
    private String targetPropertyFile = "";
    private String targetDatabaseDriver = "";
    private String targetConnectionURL = "";
    private String targetUserName = "";
    private String targetPassWord = "";
    private String targetName = "";
    private String targetTable = "";
    private boolean preserveDataOption = false;
    
    // Mapping properties
    private String mappingDefFile = "";
    private String[] sourceMapColumns = null;
    private String[] targetMapColumns = null;
    private String[] targetDefaultColumns = null;
    private String[] targetDefaultValues = null;
    
    // Execution properties
    private int commitFrequency;

    // Declarations of internally used variables
    private Properties sourceProperties = null;
    private Properties targetProperties = null;
    private String[] commonColumns = null;
    private Connection sourceCon = null;
    private Connection targetCon = null;
    private ResultSet sourceRS = null;
    private PreparedStatement sourceStmt= null;
    
    // Constructor
    public TableCopyBean() {
        super();
    }

    // Set source properties methods
    public void setSourcePropertyFile(String pr) {
    	sourcePropertyFile = pr;
    }
    
    public void setSourceName(String sn) {
        sourceName = sn;
    }

    public void setSourceDatabaseDriver(String dd) {
        sourceDatabaseDriver = dd;
    }

    public void setSourceConnectionURL(String cu) {
        sourceConnectionURL = cu;
    }

    public void setSourceUserName(String un) {
        sourceUserName = un;
    }

    public void setSourcePassWord(String pw) {
        sourcePassWord = pw;
    }

    public void setSourceTable(String ta) {
        sourceTable = ta;
    }

    public void setSourceQuery(String sq) {
        sourceQuery = sq;
    }

    public void setQueryParameters(String[] qp) {
        queryParameters = qp;
    }

    // Set target properties methods
    public void setTargetPropertyFile(String pr) {
    	targetPropertyFile = pr;
    }
    
    public void setTargetName(String sn) {
        targetName = sn;
    }

    public void setTargetDatabaseDriver(String dd) {
        targetDatabaseDriver = dd;
    }

    public void setTargetConnectionURL(String cu) {
        targetConnectionURL = cu;
    }

    public void setTargetUserName(String un) {
        targetUserName = un;
    }

    public void setTargetPassWord(String pw) {
        targetPassWord = pw;
    }

    public void setTargetTable(String ta) {
        targetTable = ta;
    }

    public void setPreserveDataOption(boolean tt) {
    	preserveDataOption = tt;
    }

    // Set optional mapping properties 
    public void setMappingDefFile(String mdf) {
    	mappingDefFile = mdf;
    }
    
    public void setSourceMapColumns(String[] smc) {
    	sourceMapColumns = smc;
    }
    
    public void setTargetMapColumns(String[] tmc) {
    	targetMapColumns = tmc;
    }
    
    public void setTargetDefaultColumns(String[] tdc) {
    	targetDefaultColumns = tdc;
    }
    
    public void setTargetDefaultValues(String[] tdv) {
    	targetDefaultValues = tdv;
    }

    // Set optional execution properties 
    public void setCommitFrequency(int cf) {
        commitFrequency = cf;
    }
    
    // Execution methods
    private void openSourceConnection() throws Exception  {
   	
    	DataSource ds = null;
    	
    	LOGGER.info("Opening source connection...");
    	
        if (sourceName == null ||sourceName.equals("")) {
        	Class.forName(sourceDatabaseDriver).newInstance();
        	LOGGER.info("Loaded database driver " + sourceDatabaseDriver);
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
    
    private String[] retrieveColumns(Connection con, String sqlText) throws Exception {

    	String[] columns = null;
       	
       	LOGGER.info("SQL: " + sqlText + ": getting columns...");
        
        PreparedStatement columnStmt = con.prepareStatement(sqlText);
        ResultSet rs = columnStmt.executeQuery();
        ResultSetMetaData rsmd = rs.getMetaData();
        columns = new String[rsmd.getColumnCount()];
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
        	columns[i - 1] = rsmd.getColumnName(i);
           	LOGGER.info("Found column: " + columns[i - 1]);
        }
        LOGGER.info("SQL: " + sqlText + ": got columns");
    	return columns;
    }

    public void retrieveColumnList() throws Exception {
    	LOGGER.info("########################################");
    	LOGGER.info("RETRIEVING COLUMN LIST...");
       	
       	String sql;
       	if (sourceQuery == null || sourceQuery.equals("")) {
       		sql = "SELECT * FROM " + sourceTable;
       	}
       	else {
       		sql = sourceQuery;
       	}
       	
    	openSourceConnection();
    	String[] sourceColumns = retrieveColumns(sourceCon,sql);
    	sourceCon.close();

    	sql = "SELECT * FROM " + targetTable;
    	openTargetConnection();
    	String[] targetColumns = retrieveColumns(targetCon,sql);
    	targetCon.close();

    	List<String> list = new ArrayList<String>();
        for (int s = 0; s < sourceColumns.length; s++) {
            for (int t = 0; t < targetColumns.length; t++) {
            	if (sourceColumns[s].equalsIgnoreCase(targetColumns[t])) {
            		list.add(sourceColumns[s]);
            	}
            }
        }
        
        commonColumns = new String[list.size()];
        list.toArray(commonColumns);
        
        LOGGER.info("COLUMN LIST RETRIEVED");
        LOGGER.info("########################################");
    }
    
    public void retrieveMappingDefinition() throws Exception {
    	
    	// Load mapping definition file
    	LOGGER.info("LOADING MAP DEFINITION FILE " + mappingDefFile + "...");
    	
    	Document mappingXML = null;
    	
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

    // Execution methods
    public void executeSelect() throws Exception {
    	LOGGER.info("########################################");
    	LOGGER.info("GETTING DATA");

    	openSourceConnection();
    	
    	String queryText;
    	
    	if (sourceQuery == null || sourceQuery.equals("")) {
    	
	    	queryText = "SELECT ";
	
	    	for (int i = 0; i < commonColumns.length; i++) {
	    		if (i > 0) {
	    			queryText += ",";
	    			}
	    		queryText += commonColumns[i];
	    	}
	    	if (sourceMapColumns!=null) {
	    		for (int i = 0; i < sourceMapColumns.length; i++) {
	    			queryText += "," + sourceMapColumns[i];
	    		}
	    	}
	    	
			queryText += " FROM " + sourceTable;
    	}
    	else {
    		queryText = sourceQuery;
    	}
    	
		LOGGER.info(queryText);
    	
        sourceStmt = sourceCon.prepareStatement(queryText);
	    sourceRS = sourceStmt.executeQuery();
	    LOGGER.fine("DATA READY");
	    
        LOGGER.info("GOT DATA");
        LOGGER.info("########################################");
    }

    public void executeInsert() throws Exception {
        LOGGER.info("########################################");
    	LOGGER.info("INSERTING DATA...");
    	
    	openTargetConnection();
    	targetCon.setAutoCommit(false);
        PreparedStatement targetStmt;
        LOGGER.info("Preserve target data = " + preserveDataOption);
        if (!preserveDataOption) {
           	targetStmt = targetCon.prepareStatement("TRUNCATE TABLE " + targetTable);
            targetStmt.executeUpdate();
            targetStmt.close();
        }
        
        String insertText = "INSERT /*+APPEND*/ INTO " + targetTable + " (";
        for (int i = 0; i < commonColumns.length; i++) {
        	if (i > 0) {
        		insertText += ",";
        	}
        	insertText += commonColumns[i];
        }
        
        if (targetMapColumns!=null) {
	       for (int i = 0; i < targetMapColumns.length; i++) {
	    	   insertText += "," + targetMapColumns[i];
	       }
	    }
	       
	    if (targetDefaultColumns!=null) {
	    	for (int i = 0; i < targetDefaultColumns.length; i++) {
	          	insertText += "," + targetDefaultColumns[i];
	        }
	    }
        
	    insertText += ") VALUES (";
	    
	    for (int i = 0; i < commonColumns.length; i++) {
	    	if (i > 0) {
	    		insertText = insertText + ",";
	    	}
	    	insertText = insertText + "?";
	    }
	    
	    if (targetMapColumns!=null) {
	    	for (int i = 0; i < targetMapColumns.length; i++) {
	    		insertText += ",?";
	    	}
	    }
	    
	    if (targetDefaultColumns!=null) {
	    	for (int i = 0; i < targetDefaultColumns.length; i++) {
	    		insertText += ",?";
	    	}
	    }
	    
	    insertText = insertText + ")";
	    
	    LOGGER.fine(insertText);
	    LOGGER.fine("Statement prepared");
	    
	    int rowCount = 0;
	    int rowSinceCommit = 0;
	    LOGGER.info("Commit every " + commitFrequency + " rows");
    	targetStmt = targetCon.prepareStatement(insertText);
    	targetStmt.setFetchSize(commitFrequency);
	    
    	//List<Object> bufferLine = null;
	    while (sourceRS.next()) {
	    	try {
	    		int position = 0;
	    		
	    		for (int i = 0; i < commonColumns.length; i++) {
	    			position++;
	    			try {
	    				targetStmt.setObject(position, sourceRS.getObject(commonColumns[i]));
	    			}
	    			
	    			catch (Exception e){
	    				targetStmt.setObject(position, null);
	    			}
	    		}
	    		
	    		if (sourceMapColumns!=null) {
	    			for (int i = 0; i < sourceMapColumns.length; i++) {
		             	position++;
		              	try {
		              		targetStmt.setObject(position, sourceRS.getObject(sourceMapColumns[i]));
		                }
		                catch (Exception e){
		                	targetStmt.setObject(position, null);
		                }
		            }
	    		}
	            
	            if (targetDefaultValues!=null) {
	            	for (int i = 0; i < targetDefaultValues.length; i++) {
	            		position++;
		                try {
		                	targetStmt.setObject(position, targetDefaultValues[i]);
		                }
		                catch (Exception e){
		                	targetStmt.setObject(position, null);
		                }
		            }
	            }
	        }
	        catch(Exception e) {
	        	LOGGER.severe("Unexpected exception, list of column values:");
	        	for (int i = 0; i < commonColumns.length; i++) {
	        		try {
	        			LOGGER.severe("########################################\n" + commonColumns[i] + " ==> " + sourceRS.getObject(commonColumns[i]).toString());
				    }
	        		catch(NullPointerException npe) {
	        			LOGGER.severe("########################################\n" + commonColumns[i]);
			        }
	            }
	            LOGGER.severe(e.getMessage());
	            throw e;
	        }
	    	targetStmt.executeUpdate();
	    	targetStmt.clearParameters();
	    	
	    	rowCount++;
	    	rowSinceCommit++;
	    	if (rowSinceCommit==commitFrequency) {
	    		targetCon.commit();
	    		rowSinceCommit = 0;
	    		LOGGER.info(rowCount + " rows inserted");
	    	}
	    }
    	targetStmt.close();
        targetCon.commit();
	    targetCon.close();

	    sourceRS.close();
	    sourceStmt.close();
	    sourceCon.close();

	    LOGGER.info(rowCount + " rows totally inserted");
	    LOGGER.info("INSERT COMPLETED");
	    LOGGER.info("########################################");
    }
}
