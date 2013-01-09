package org.openbusinessintelligence.tools.file;

import java.io.*;
import java.util.*;
import java.util.zip.*;

public class FileMergeBean {

	private final static java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(FileMergeBean.class.getPackage().getName());

    // Declarations of bean properties
	private String inputZipFile = "";
	private String inputDirectory = "";
	private String[] outputFileNames = null;
	private String[] distributionPatterns = null;
	private boolean addFileNameOption = false;
	private String columnSeparator = "";

    // Constructor
	public FileMergeBean() {
		super();
	}
	
    // Set properties methods
	public void setInputZipFile(String property) {
		inputZipFile = property;
	}
	
	public void setInputDirectory(String property) {
		inputDirectory = property;
	}
	
	public void setOutputFileNames(String[] property) {
		outputFileNames = property;
	}
	
	public void setDistributionPatterns(String[] property) {
		distributionPatterns = property;
	}
	
	public void setAddFileNameOption(boolean property) {
		addFileNameOption = property;
	}
	
	public void setColumnSeparator(String property) {
		columnSeparator = property;
	}
	
    // Execution methods
	public void mergeFiles() throws Exception {

		ArrayList<BufferedWriter> outputFiles = new ArrayList<BufferedWriter>();
		for (int i=0; i<outputFileNames.length; i++) {
			FileOutputBean outputFile = new FileOutputBean();
			outputFile.setFileName(outputFileNames[i]);
			outputFiles.add(new BufferedWriter(outputFile.getWriter()));
		}
		
    	if (!(inputZipFile == null || inputZipFile.equals("")) ) {
    		
        	LOGGER.info("Reading entries in zip file " + inputZipFile);
    		
    		ZipFile zipFile = new ZipFile(inputZipFile);
    		Enumeration entries = zipFile.entries();
    		
    		while(entries.hasMoreElements()) {
    			ZipEntry entry = (ZipEntry)entries.nextElement();
    			if(!entry.isDirectory()) {
    				LOGGER.info("IMPORTING ENTRY " + entry.getName());
    				BufferedReader reader = new BufferedReader(new InputStreamReader(zipFile.getInputStream(entry)));
    				
    				String line;
    				while ((line=reader.readLine()) != null) {
	    				for (int i=0; i<outputFileNames.length; i++) {
	    					if (line.startsWith(distributionPatterns[i]) ) {
	    						if (addFileNameOption) {
	    							line = entry.getName() + columnSeparator + line;
	    						}
	    						outputFiles.get(i).write(line);
	    						outputFiles.get(i).newLine();
	    					}
	    				}
    				}
    				
            		reader.close();
    				LOGGER.info("ENTRY " + entry.getName() + " IMPORTED");
    			}
    		}
    		
        	LOGGER.info("ZIP FILE " + inputZipFile + " COMPLETED");
    	}
		for (int i=0; i<outputFileNames.length; i++) {
			outputFiles.get(i).close();
		}

	}
}
