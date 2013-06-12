package org.openbusinessintelligence.tools.db;

import java.util.*;
import java.util.zip.*;

import org.slf4j.LoggerFactory;

public class ImportCsvSeriesBean {
	
	static final org.slf4j.Logger logger = LoggerFactory.getLogger(ImportCsvSeriesBean.class);

    // Declarations of bean properties
	// Source properties
    private String sourcePropertyFile = "";
    private String sourceDatabaseDriver = "";
    private String sourceZipFile = "";
    private String sourceDirectory = "";
    private String sourceWhereClause = "";

    // Target properties
    private String targetPropertyFile = "";
    private String targetDatabaseDriver = "";
    private String targetConnectionURL = "";
    private String targetUserName = "";
    private String targetPassWord = "";
    private String targetName = "";
    private String targetTable = "";
	private String fileNameColumn = "";
    
    // Execution properties
    private int commitFrequency;
	
	/**
	 * Constructor
	 */
	public ImportCsvSeriesBean() {
		super();
	}

    // Set source properties methods
    public void setSourcePropertyFile(String pr) {
    	sourcePropertyFile = pr;
    }

    public void setSourceDatabaseDriver(String dd) {
        sourceDatabaseDriver = dd;
    }

    public void setSourceZipFile(String szf) {
    	sourceZipFile = szf;
    }

    public void setSourceDirectory(String sd) {
        sourceDirectory = sd;
    }

    public void setSourceWhereClause(String swc) {
        sourceWhereClause = swc;
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

    public void setTargetTable(String tt) {
        targetTable = tt;
    }

    public void setFileNameColumn(String fnc) {
    	fileNameColumn = fnc;
    }
    // Set optional execution properties 
    public void setCommitFrequency(int cf) {
        commitFrequency = cf;
    }
    
    // Import a series of zip files
    public void importCsvSeries() throws Exception {
    	
		org.openbusinessintelligence.tools.db.DataCopyBean dataCopy = new org.openbusinessintelligence.tools.db.DataCopyBean();

		dataCopy.setSourcePropertyFile(sourcePropertyFile);
		dataCopy.setSourceDatabaseDriver(sourceDatabaseDriver);
		dataCopy.setSourceUserName("");
		dataCopy.setSourcePassWord("");

		dataCopy.setTargetPropertyFile(targetPropertyFile);
		dataCopy.setTargetDatabaseDriver(targetDatabaseDriver);
		dataCopy.setTargetConnectionURL(targetConnectionURL);
		dataCopy.setTargetUserName(targetUserName);
		dataCopy.setTargetPassWord(targetPassWord);
		dataCopy.setTargetTable(targetTable);		
		dataCopy.setPreserveDataOption(true);
		
		String[] fileNameCol = new String[1];
		String[] fileNameValue = new String[1];
		
		if (!(fileNameColumn == null || fileNameColumn.equals("")) ) {
			fileNameCol[0] = fileNameColumn;
		}
		
		dataCopy.setCommitFrequency(commitFrequency);
    	
    	if (!(sourceZipFile == null || sourceZipFile.equals("")) ) {
    		
        	logger.info("LOADING ENTRIES IN ZIP FILE " + sourceZipFile);
        	dataCopy.setSourceConnectionURL("jdbc:relique:csv:zip:" + sourceZipFile);
    		
    		// Loop on zip entries
    		ZipFile zipFile = new ZipFile(sourceZipFile);    		
    		Enumeration<? extends ZipEntry> entries = zipFile.entries();
    		
    		int i = 0;
    		while(entries.hasMoreElements()) {
    			ZipEntry entry = (ZipEntry)entries.nextElement();
    			if(!entry.isDirectory()) {
    				
    				logger.info("IMPORTING ENTRY " + entry.getName());
    				
    				dataCopy.setSourceQuery("SELECT * FROM " + entry.getName() + " WHERE " + sourceWhereClause);
            		
            		if (i == 0) {
            			if (!(fileNameColumn == null || fileNameColumn.equals("")) ) {
            				dataCopy.setTargetDefaultColumns(fileNameCol);
            			}
            			dataCopy.retrieveColumnList();
            		}

            		if (!(fileNameColumn == null || fileNameColumn.equals("")) ) {
	            		fileNameValue[0] = entry.getName();
	            		dataCopy.setTargetDefaultValues(fileNameValue);
	            	}
            		
            		dataCopy.executeSelect();
            		dataCopy.executeInsert();
        			i += 1;
    				logger.info("ENTRY " + entry.getName() + " IMPORTED");
    			}
    		}
    		zipFile.close();
    		
        	logger.info("ZIP FILE " + sourceZipFile + " COMPLETED");
    	}
	}
    
}
