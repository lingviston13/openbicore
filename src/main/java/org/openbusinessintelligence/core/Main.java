package org.openbusinessintelligence.core;

import java.io.*;
import java.util.*;
import javax.xml.parsers.*;

import org.slf4j.*;
import org.w3c.dom.*;
import org.apache.commons.cli.*;

import org.openbusinessintelligence.core.file.*;
import org.openbusinessintelligence.core.xml.*;

public class Main {
	
	static final org.slf4j.Logger logger = LoggerFactory.getLogger(Main.class);
	
	private static Options cmdOptions;
	private static Properties properties;
	private static CommandLine cmd;
	
	/**
	 * @param args
	 * Arguments
	 */	
	public static void main(String[] args) throws Exception {
		logger.info("###################################################################");
		logger.info("START");
		
		configureCmdOptions();
		CommandLineParser parser = new PosixParser();
		try {
			// parse the command line arguments
			cmd = parser.parse(cmdOptions, args);
		}
		catch(Exception e) {
			logger.error("Unexpected exception:" + e.toString());
		    throw e;
		}
		
	    if (cmd.hasOption("help")) {
	        // print help
			HelpFormatter formatter = new HelpFormatter();
			formatter.setWidth(300);
			formatter.printHelp("obiTools",cmdOptions);
	    }
	    if (cmd.hasOption("propertyfile")) {
	    	// Get options from optional property file
            try {
		    	properties = new Properties();
		    	properties.load(new FileInputStream(cmd.getOptionValue("propertyfile")));
            }
    		catch(Exception e) {
    			logger.error("Cannot read property file:\n" + e.getMessage());
    		    throw e;
    		}
	    }
	    if (cmd.hasOption("function")) {
	    	
	    	// Do something depending on function
	    	String function = cmd.getOptionValue("function");
	    	if (function.equalsIgnoreCase("toadproject")) {
	    		// Generate a TOAD project file
				org.openbusinessintelligence.core.toad.ToadProjectFileCreator toadProjectCreator = new org.openbusinessintelligence.core.toad.ToadProjectFileCreator();
				toadProjectCreator.createProject(
						getOption("toadprojectname"),
						getOption("toadprojectfolder"),
						getOption("toadprojectfileslocation")
				);
	    	}
	    	if (function.equalsIgnoreCase("wrapperscript")) {
	    		// Generate a Wrapper Script
				org.openbusinessintelligence.core.script.WrapperScriptCreator wrapperScriptCreator = new org.openbusinessintelligence.core.script.WrapperScriptCreator();
				wrapperScriptCreator.createWrapper(
						getOption("wrapperscript"),
						getOption("rootfolder"),
						getOption("defaultsubfolders")
				);
	    	}
	    	if (function.equalsIgnoreCase("dwsodeploy")) {
	    		// Generate deployment file
				org.openbusinessintelligence.core.script.InstallScriptCreator dwsoInstallScriptCreator = new org.openbusinessintelligence.core.script.InstallScriptCreator();
				dwsoInstallScriptCreator.createScript(
						getOption("deployfilename"),
						getOption("deployfileforlder"),
						getOption("dwsofolder"),
						getOption("dwsoinstallfile")
				);
	    	}
	    	if (function.equalsIgnoreCase("mail")) {
	    		// Send an email
	    		org.openbusinessintelligence.core.mail.MailBean mailSender = new org.openbusinessintelligence.core.mail.MailBean();
	    		String mailContent = null;
	    		if (cmd.hasOption("mailcontentsource")) {
		    		if (getOption("mailcontentsource").equalsIgnoreCase("database")) {
						org.openbusinessintelligence.core.db.QueryBean query = new org.openbusinessintelligence.core.db.QueryBean();
						query.setDatabaseDriver(getOption("dbdriverclass"));
						query.setConnectionURL(getOption("dbconnectionurl"));
						query.setUserName(getOption("dbusername"));
						query.setPassWord(getOption("dbpassword"));
						query.setQueryText(getOption("dbquery"));
						ByteArrayInputStream bufferIn = null;
						if (getOption("dboutputformat").equalsIgnoreCase("wrs")) {
							// Get the stream from the webrowset
					    	ByteArrayOutputStream bufferOut = new ByteArrayOutputStream();
					    	query.generate();
					    	query.setStream(bufferOut);
					    	query.streamWRS();
					    	bufferIn = new ByteArrayInputStream(bufferOut.toByteArray());
						}
						if (getOption("dboutputformat").equalsIgnoreCase("raw")) {
					    	bufferIn = new ByteArrayInputStream(query.getRawOutput().getBytes());
						}
				    	if (cmd.hasOption("mailcontenttype")) {

							// Load the stylesheet
						    FileInputBean fileInput = new FileInputBean();
							fileInput = new FileInputBean();
							fileInput.setDirectoryName("xsl");
							fileInput.setFileName(getOption("mailcontentformat") + ".xsl");

							// Get the transformed xml
					    	ByteArrayOutputStream transformOut = new ByteArrayOutputStream();
							TransformerBean transformer = new TransformerBean();
							transformer.setStyleSheet(fileInput.getReader());
							transformer.setStreamInput(bufferIn);
							transformer.setStreamOutput(transformOut);
							transformer.transform();
					    	
					    	if (getOption("mailcontenttype").equalsIgnoreCase("html")) {
					    		mailContent = "";
					    		if (cmd.hasOption("mailcontentstyle")) {
					    			FileInputBean file = new FileInputBean();
									file.setDirectoryName("css");
									file.setFileName(getOption("mailcontentstyle") + ".css");
					    			mailContent += "<head><style>" + file.getString() + "</style></head>";
					    		}
					    		mailContent += "<body>" + transformOut.toString() + "</body>";
					    	}
				    	}
				    	else {
				    		mailContent = bufferIn.toString();
				    	}
		    		}
		    		if (getOption("mailcontentsource").equalsIgnoreCase("file")) {
						org.openbusinessintelligence.core.file.FileInputBean file = new org.openbusinessintelligence.core.file.FileInputBean();
						file.setDirectoryName(getOption("infilefolder"));
						file.setFileName(getOption("infilename"));
						mailContent = file.getString();
		    		}
	    		}
	    		else {
	    			mailContent = getOption("mailcontent");
	    		}
	    		mailSender.setSmtpServer(getOption("mailhost"));
	    		mailSender.setSenderAddress(getOption("mailfrom"));
	    		mailSender.setReceiverList(getOption("mailto"));
	    		mailSender.setMailSubject(getOption("mailsubject"));
	    		mailSender.setMailContent(mailContent);
	    		mailSender.sendMail();
	    	}
	    	if (function.equalsIgnoreCase("executeprocedure")) {
	    		// Execute a store procedure
				org.openbusinessintelligence.core.db.ProcedureBean procedure = new org.openbusinessintelligence.core.db.ProcedureBean();
				
				procedure.setPropertyFile(getOption("dbconnpropertyfile"));
				procedure.setDatabaseDriver(getOption("dbdriverclass"));
				procedure.setConnectionURL(getOption("dbconnectionurl"));
				procedure.setUserName(getOption("dbusername"));
				procedure.setPassWord(getOption("dbpassword"));
				procedure.setProcedureName(getOption("dbprocedure"));
				
				try {
					procedure.openConnection();
					procedure.execute();
				}
				catch (Exception e) {
					logger.error("UNEXPECTED EXCEPTION");
					logger.error(e.getMessage());
				    throw e;
				}
	    	}
	    	if (function.equalsIgnoreCase("dbproperties")) {
				logger.info("Get database properties");
	    		// Get database properties
	    		org.openbusinessintelligence.core.db.ConnectionBean connectionBean = new org.openbusinessintelligence.core.db.ConnectionBean();
	    		connectionBean.setPropertyFile(getOption("dbconnpropertyfile"));
	    		connectionBean.setKeyWordFile(getOption("dbconnkeywordfile"));
	    		connectionBean.setDatabaseDriver(getOption("dbdriverclass"));
	    		connectionBean.setConnectionURL(getOption("dbconnectionurl"));
	    		connectionBean.setUserName(getOption("dbusername"));
	    		connectionBean.setPassWord(getOption("dbpassword"));
				
				try {
					connectionBean.openConnection();
				}
				catch (Exception e) {
					logger.error("UNEXPECTED EXCEPTION");
					logger.error(e.getMessage());
				    throw e;
				}
				connectionBean.closeConnection();
				logger.info("Properties retrieved");
	    	}
	    	if (function.equalsIgnoreCase("tablecopy")) {
				logger.info("Copy an entire schema, a single table or the result of a query from a database to another");
				
	    		org.openbusinessintelligence.core.db.ConnectionBean sourceConnectionBean = new org.openbusinessintelligence.core.db.ConnectionBean();
	    		sourceConnectionBean.setPropertyFile(getOption("srcdbconnpropertyfile"));
	    		sourceConnectionBean.setKeyWordFile(getOption("srcdbconnkeywordfile"));
	    		sourceConnectionBean.setDatabaseDriver(getOption("srcdbdriverclass"));
	    		sourceConnectionBean.setConnectionURL(getOption("srcdbconnectionurl"));
	    		sourceConnectionBean.setUserName(getOption("srcdbusername"));
	    		sourceConnectionBean.setPassWord(getOption("srcdbpassword"));
	    		sourceConnectionBean.openConnection();
	    		
	    		org.openbusinessintelligence.core.db.ConnectionBean targetConnectionBean = new org.openbusinessintelligence.core.db.ConnectionBean();
	    		targetConnectionBean.setPropertyFile(getOption("trgdbconnpropertyfile"));
	    		targetConnectionBean.setKeyWordFile(getOption("trgdbconnkeywordfile"));
	    		targetConnectionBean.setDatabaseDriver(getOption("trgdbdriverclass"));
	    		targetConnectionBean.setConnectionURL(getOption("trgdbconnectionurl"));
	    		targetConnectionBean.setUserName(getOption("trgdbusername"));
	    		targetConnectionBean.setPassWord(getOption("trgdbpassword"));
	    		
				logger.info("Source and target connections prepared");
				
	    		String sourceSchema = getOption("sourceschema");
	    		logger.info("Source schema: " + sourceSchema);
	    		String sourceTable = getOption("sourcetable");
	    		logger.info("Source table: " + sourceTable);
	    		String sourceQuery = getOption("sourcequery");
	    		logger.info("Source query: " + sourceQuery);
	    		String targetSchema = getOption("targetschema");
	    		logger.info("Target schema: " + targetSchema);
	    		String targetTable = getOption("targettable");
	    		logger.info("Target table: " + targetTable);
	    		String[] sourceTableList = null;
	    		String[] targetTableList = null;

				sourceConnectionBean.setSchemaName(sourceSchema);
	    		if ((sourceSchema != null) &&
	    			(sourceTable == null || sourceSchema.equals("")) &&
	    			(sourceQuery == null || sourceSchema.equals(""))
	    		) {
					logger.info("Copy all objects of a schema");
					sourceTableList = sourceConnectionBean.getTableList();
					targetTableList = sourceTableList;
	    		}
	    		else {
					logger.info("Copy a single table or the result of a query");
					sourceTableList = new String[1];
					targetTableList = new String[1];
					sourceTableList[0] = sourceTable;
					targetTableList[0] = targetTable;
	    		}
	    		
				try {
					if (Boolean.parseBoolean(getOption("trgcreate"))) {
						logger.info("Create tables if they don't exist");
						// Open target connection
			    		targetConnectionBean.openConnection();
						// Get source dictionary
			    		org.openbusinessintelligence.core.db.DataDictionaryBean dictionaryBean = new org.openbusinessintelligence.core.db.DataDictionaryBean();
			    		org.openbusinessintelligence.core.db.TableCreateBean tableCreate = new org.openbusinessintelligence.core.db.TableCreateBean();
			    		dictionaryBean.setSourceConnection(sourceConnectionBean);
		    			for (int i = 0; i < sourceTableList.length; i++ ) {
							logger.info("Creating table: " + sourceTableList[i]);
				    		dictionaryBean.setSourceTable(sourceTableList[i]);
				    		dictionaryBean.setSourceQuery(sourceQuery);
				    		//
				    		dictionaryBean.setTargetConnection(targetConnectionBean);
				    		//
				    		dictionaryBean.retrieveColumns();
				    		String[] columns = dictionaryBean.getTargetColumnNames();
				    		String[] columnDefs = dictionaryBean.getTargetColumnDefinitions();
				    		// Create a table basing on the result
				    		tableCreate.setTargetConnection(targetConnectionBean);
				    		tableCreate.setTargetSchema(targetSchema);
				    		tableCreate.setTargetTable(targetTableList[i]);
				    		tableCreate.setTargetColumns(columns);
				    		tableCreate.setTargetColumnDefinitions(columnDefs);
				    		tableCreate.setDropIfExistsOption(Boolean.parseBoolean(getOption("dropifexists")));
				    		tableCreate.createTable();
		    			}
		    			targetConnectionBean.closeConnection();
					}
					// Open target connection
		    		targetConnectionBean.openConnection();
	    			for (int i = 0; i < targetTableList.length; i++ ) {
			    		// Copy the content of a source sql query into a target rdbms table
						logger.info("Feeding table: " + sourceTableList[i]);
						org.openbusinessintelligence.core.db.DataCopyBean dataCopy = new org.openbusinessintelligence.core.db.DataCopyBean();
						dataCopy.setSourceConnection(sourceConnectionBean);
						dataCopy.setSourceTable(sourceTableList[i]);
						dataCopy.setSourceQuery(sourceQuery);
						
						dataCopy.setTargetConnection(targetConnectionBean);
						dataCopy.setTargetSchema(targetSchema);
						dataCopy.setTargetTable(targetTableList[i]);
						dataCopy.setPreserveDataOption(Boolean.parseBoolean(getOption("trgpreservedata")));
						
						String mappingDefFile = getOption("mapdeffile");
						dataCopy.setMappingDefFile(mappingDefFile);
						
						if (getOption("commitfrequency") != null) {
							dataCopy.setCommitFrequency(Integer.parseInt(getOption("commitfrequency")));
						}
						if (mappingDefFile!=null) {
							dataCopy.retrieveMappingDefinition();
						}
						dataCopy.retrieveColumnList();
						dataCopy.executeSelect();
						dataCopy.executeInsert();
	    			}
	    			// Close target connection
					targetConnectionBean.closeConnection();
	    			// Close source connection
					sourceConnectionBean.closeConnection();
				}
				catch (Exception e) {
					logger.error("UNEXPECTED EXCEPTION");
					logger.error(e.getMessage());
					try {
						sourceConnectionBean.closeConnection();
					}
					finally {
						
					}
					try {
						targetConnectionBean.closeConnection();
					}
					finally {
						
					}
				    throw e;
				}
	    	}
	    	if (function.equalsIgnoreCase("importcsvseries")) {
	    	
	    		String sourceZipFile = getOption("sourcezipfile");
	    		if (!(sourceZipFile == null || sourceZipFile.equals(""))) {
		    		org.openbusinessintelligence.core.db.ConnectionBean sourceConnectionBean = new org.openbusinessintelligence.core.db.ConnectionBean();
		    		sourceConnectionBean.setPropertyFile(getOption("srcdbconnpropertyfile"));
		    		sourceConnectionBean.setDatabaseDriver(getOption("srcdbdriverclass"));
		    		sourceConnectionBean.setConnectionURL("jdbc:relique:csv:zip:" + sourceZipFile);
		    		sourceConnectionBean.setUserName(getOption("srcdbusername"));
		    		sourceConnectionBean.setPassWord(getOption("srcdbpassword"));	 
		    		//sourceConnectionBean.openConnection();
					
		    		org.openbusinessintelligence.core.db.ConnectionBean targetConnectionBean = new org.openbusinessintelligence.core.db.ConnectionBean();
		    		targetConnectionBean.setPropertyFile(getOption("trgdbconnpropertyfile"));
		    		targetConnectionBean.setDatabaseDriver(getOption("trgdbdriverclass"));
		    		targetConnectionBean.setConnectionURL(getOption("trgdbconnectionurl"));
		    		targetConnectionBean.setUserName(getOption("trgdbusername"));
		    		targetConnectionBean.setPassWord(getOption("trgdbpassword"));
		    		targetConnectionBean.openConnection();
		    		
		    		// Import a series of csv files with the same structure in the same table
					org.openbusinessintelligence.core.db.ImportCsvSeriesBean importCsvSeries = new org.openbusinessintelligence.core.db.ImportCsvSeriesBean();
					importCsvSeries.setSourceConnection(sourceConnectionBean);
					importCsvSeries.setSourceZipFile(sourceZipFile);
					importCsvSeries.setSourceWhereClause(getOption("sourcewhereclause"));
		    		importCsvSeries.setTargetConnection(targetConnectionBean);
					importCsvSeries.setTargetTable(getOption("targettable"));	
					importCsvSeries.setFileNameColumn(getOption("filenamecolumn"));
									
					importCsvSeries.setCommitFrequency(Integer.parseInt(getOption("commitfrequency")));
					
					try {
						importCsvSeries.importCsvSeries();
						targetConnectionBean.closeConnection();
					}
					catch (Exception e) {
						logger.error("UNEXPECTED EXCEPTION");
						logger.error(e.getMessage());
						try {
							targetConnectionBean.closeConnection();
						}
						finally {
							
						}
					    throw e;
					}
	        	}
	    	}
	    	if (function.equalsIgnoreCase("mergefiles")) {
				org.openbusinessintelligence.core.file.FileMergeBean fileMerge = new org.openbusinessintelligence.core.file.FileMergeBean();
				fileMerge.setInputZipFile(getOption("sourcezipfile"));
				fileMerge.setInputDirectory(getOption("sourcedirectory"));
				fileMerge.setOutputFileNames(getOption("outputfilenamelist").split(","));
				fileMerge.setDistributionPatterns(getOption("distributionpattern").split(","));
				fileMerge.setAddFileNameOption(Boolean.parseBoolean(getOption("filenameoption")));
				fileMerge.setColumnSeparator(getOption("columnseparator"));
				
				try {
					fileMerge.mergeFiles();
				}
				catch (Exception e) {
					logger.error("UNEXPECTED EXCEPTION");
					logger.error(e.getMessage());
				    throw e;
				}
	    	}
		}
	    logger.info("FINISH");
	    logger.info("###################################################################");
	}
	
	@SuppressWarnings("static-access")
	private static void configureCmdOptions() throws Exception {
		logger.info("Configure command line options");
		
		cmdOptions = new Options();		
		Option option = new Option("help", "Print this message");
		cmdOptions.addOption(option);
		
		org.w3c.dom.Document optionsXML = null;
		try {
			DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
			javax.xml.parsers.DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
			optionsXML = docBuilder.parse(Thread.currentThread().getContextClassLoader().getResource("cmd/coreCmdOptions.xml").toString());
			optionsXML.getDocumentElement().normalize();
		}
		catch(Exception e) {
			logger.error("Cannot load option file:\n" + e.getMessage());
		    throw e;
		}
		NodeList nList = optionsXML.getElementsByTagName("option");
 		for (int temp = 0; temp < nList.getLength(); temp++) {
 			Node nNode = nList.item(temp);
 			if (nNode.getNodeType() == Node.ELEMENT_NODE) {
 				Element eElement = (Element) nNode;
 				//System.out.println("Option : " + eElement.getElementsByTagName("name").item(0).getChildNodes().item(0).getNodeValue());
 				option = OptionBuilder.hasArg()
 						.withArgName(eElement.getElementsByTagName("argName").item(0).getChildNodes().item(0).getNodeValue())
 		                .withDescription(eElement.getElementsByTagName("description").item(0).getChildNodes().item(0).getNodeValue())
 		                .create(eElement.getElementsByTagName("name").item(0).getChildNodes().item(0).getNodeValue());
 				cmdOptions.addOption(option);
		   }
		}
		logger.info("Options configured");
	}
	
	private static String getOption(String optionName) {
		String optionValue = null;

		if (
			cmd.getOptionValue(optionName) == null ||
			cmd.getOptionValue(optionName).equalsIgnoreCase("")
		) {
			try {
				optionValue = properties.getProperty(optionName);			
			}
			catch(NullPointerException npe) {
				
			}
		}
		else {
			optionValue = cmd.getOptionValue(optionName);
		}
		
		return optionValue;
	}
	
}
