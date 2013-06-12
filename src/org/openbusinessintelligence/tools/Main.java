package org.openbusinessintelligence.tools;

import java.io.*;
import java.util.*;
import javax.xml.parsers.*;

import org.slf4j.*;
import org.w3c.dom.*;
import org.apache.commons.cli.*;

import org.openbusinessintelligence.tools.file.*;
import org.openbusinessintelligence.tools.xml.*;
//import org.openbusinessintelligence.tools.script.*;

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
			logger.error("Unexpected exception:" + e.getMessage());
		    throw e;
		}
		
	    if (cmd.hasOption("help")) {
	        // print help
			HelpFormatter formatter = new HelpFormatter();
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
				org.openbusinessintelligence.tools.toad.ToadProjectFileCreator toadProjectCreator = new org.openbusinessintelligence.tools.toad.ToadProjectFileCreator();
				toadProjectCreator.createProject(
						getOption("toadprojectname"),
						getOption("toadprojectfolder"),
						getOption("toadprojectfileslocation")
				);
	    	}
	    	if (function.equalsIgnoreCase("wrapperscript")) {
	    		// Generate a TOAD project file
				org.openbusinessintelligence.tools.script.WrapperScriptCreator wrapperScriptCreator = new org.openbusinessintelligence.tools.script.WrapperScriptCreator();
				wrapperScriptCreator.createWrapper(
						getOption("wrapperscript"),
						getOption("rootfolder"),
						getOption("defaultsubfolders")
				);
	    	}
	    	if (function.equalsIgnoreCase("dwsodeploy")) {
	    		// Generate deployment file
				org.openbusinessintelligence.tools.script.InstallScriptCreator dwsoInstallScriptCreator = new org.openbusinessintelligence.tools.script.InstallScriptCreator();
				dwsoInstallScriptCreator.createScript(
						getOption("deployfilename"),
						getOption("deployfileforlder"),
						getOption("dwsofolder"),
						getOption("dwsoinstallfile")
				);
	    	}
	    	if (function.equalsIgnoreCase("mail")) {
	    		// Send an email
	    		org.openbusinessintelligence.tools.mail.MailBean mailSender = new org.openbusinessintelligence.tools.mail.MailBean();
	    		String mailContent = null;
	    		if (cmd.hasOption("mailcontentsource")) {
		    		if (getOption("mailcontentsource").equalsIgnoreCase("database")) {
						org.openbusinessintelligence.tools.db.QueryBean query = new org.openbusinessintelligence.tools.db.QueryBean();
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
						org.openbusinessintelligence.tools.file.FileInputBean file = new org.openbusinessintelligence.tools.file.FileInputBean();
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
				org.openbusinessintelligence.tools.db.ProcedureBean procedure = new org.openbusinessintelligence.tools.db.ProcedureBean();
				
				procedure.setPropertyFile(getOption("dbconnaddpropertyfile"));
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
	    	if (function.equalsIgnoreCase("tablecopy")) {
				
				try {
					if (Boolean.parseBoolean(getOption("trgcreate"))) {
						// Get source dictionary
			    		org.openbusinessintelligence.tools.db.DataDictionaryBean dictionaryBean = new org.openbusinessintelligence.tools.db.DataDictionaryBean();
			    		dictionaryBean.setSourcePropertyFile(getOption("srcconnaddpropertyfile"));
			    		dictionaryBean.setSourceDatabaseDriver(getOption("srcdbdriverclass"));
			    		dictionaryBean.setSourceConnectionURL(getOption("srcdbconnectionurl"));
			    		dictionaryBean.setSourceUserName(getOption("srcdbusername"));
			    		dictionaryBean.setSourcePassWord(getOption("srcdbpassword"));
			    		dictionaryBean.setSourceTable(getOption("sourcetable"));
			    		dictionaryBean.setSourceQuery(getOption("sourcequery"));
			    		//
			    		dictionaryBean.setTargetPropertyFile(getOption("trgconnaddpropertyfile"));
			    		dictionaryBean.setTargetDatabaseDriver(getOption("trgdbdriverclass"));
			    		dictionaryBean.setTargetConnectionURL(getOption("trgdbconnectionurl"));
			    		dictionaryBean.setTargetUserName(getOption("trgdbusername"));
			    		dictionaryBean.setTargetPassWord(getOption("trgdbpassword"));
			    		dictionaryBean.setTargetTable(getOption("targettable"));
			    		//
			    		dictionaryBean.retrieveColumns();
			    		String[] columns = dictionaryBean.getTargetColumnNames();
			    		String[] columnDefs = dictionaryBean.getTargetColumnDefinitions();

			    		// Create a table basing on the result
			    		org.openbusinessintelligence.tools.db.TableCreateBean tableCreate = new org.openbusinessintelligence.tools.db.TableCreateBean();
						
						tableCreate.setTargetPropertyFile(getOption("trgconnaddpropertyfile"));
			    		tableCreate.setTargetDatabaseDriver(getOption("trgdbdriverclass"));
			    		tableCreate.setTargetConnectionURL(getOption("trgdbconnectionurl"));
			    		tableCreate.setTargetUserName(getOption("trgdbusername"));
			    		tableCreate.setTargetPassWord(getOption("trgdbpassword"));
			    		tableCreate.setTargetTable(getOption("targettable"));

			    		tableCreate.setTargetColumns(columns);
			    		tableCreate.setTargetColumnDefinitions(columnDefs);
			    		tableCreate.createTable();
					}
					
		    		// Copy the content of a source sql query into a target rdbms table
					org.openbusinessintelligence.tools.db.DataCopyBean dataCopy = new org.openbusinessintelligence.tools.db.DataCopyBean();

					dataCopy.setSourcePropertyFile(getOption("srcconnaddpropertyfile"));
					dataCopy.setSourceDatabaseDriver(getOption("srcdbdriverclass"));
					dataCopy.setSourceConnectionURL(getOption("srcdbconnectionurl"));
					dataCopy.setSourceUserName(getOption("srcdbusername"));
					dataCopy.setSourcePassWord(getOption("srcdbpassword"));
					dataCopy.setSourceTable(getOption("sourcetable"));
					dataCopy.setSourceQuery(getOption("sourcequery"));
					
					dataCopy.setTargetPropertyFile(getOption("trgconnaddpropertyfile"));
					dataCopy.setTargetDatabaseDriver(getOption("trgdbdriverclass"));
					dataCopy.setTargetConnectionURL(getOption("trgdbconnectionurl"));
					dataCopy.setTargetUserName(getOption("trgdbusername"));
					dataCopy.setTargetPassWord(getOption("trgdbpassword"));
					dataCopy.setTargetTable(getOption("targettable"));
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
				catch (Exception e) {
					logger.error("UNEXPECTED EXCEPTION");
					logger.error(e.getMessage());
				    throw e;
				}
	    	}
	    	if (function.equalsIgnoreCase("importcsvseries")) {
	    	
	    		// Import a series of csv files with the same structure in the same table
				org.openbusinessintelligence.tools.db.ImportCsvSeriesBean importCsvSeries = new org.openbusinessintelligence.tools.db.ImportCsvSeriesBean();
				importCsvSeries.setSourcePropertyFile(getOption("srcconnaddpropertyfile"));
				importCsvSeries.setSourceDatabaseDriver(getOption("srcdbdriverclass"));
				importCsvSeries.setSourceZipFile(getOption("sourcezipfile"));
				importCsvSeries.setSourceDirectory(getOption("sourcedirectory"));
				importCsvSeries.setSourceWhereClause(getOption("sourcewhereclause"));
				
				importCsvSeries.setTargetPropertyFile(getOption("trgconnaddpropertyfile"));
				importCsvSeries.setTargetDatabaseDriver(getOption("trgdbdriverclass"));
				importCsvSeries.setTargetConnectionURL(getOption("trgdbconnectionurl"));
				importCsvSeries.setTargetUserName(getOption("trgdbusername"));
				importCsvSeries.setTargetPassWord(getOption("trgdbpassword"));
				importCsvSeries.setTargetTable(getOption("targettable"));	
				importCsvSeries.setFileNameColumn(getOption("filenamecolumn"));
								
				importCsvSeries.setCommitFrequency(Integer.parseInt(getOption("commitfrequency")));
				
				try {
					importCsvSeries.importCsvSeries();
				}
				catch (Exception e) {
					logger.error("UNEXPECTED EXCEPTION");
					logger.error(e.getMessage());
				    throw e;
				}
	    	}
	    	/*if (function.equalsIgnoreCase("importdbdictionary")) {
				org.openbusinessintelligence.tools.db.DataDictionaryBean dataDictionary = new org.openbusinessintelligence.tools.db.DataDictionaryBean();
				
				// Import dictionary information about a table or query into a target table
				dataDictionary.setSourcePropertyFile(getOption("srcconnaddpropertyfile"));
				dataDictionary.setSourceDatabaseDriver(getOption("srcdbdriverclass"));
				dataDictionary.setSourceConnectionURL(getOption("srcdbconnectionurl"));
				dataDictionary.setSourceUserName(getOption("srcdbusername"));
				dataDictionary.setSourcePassWord(getOption("srcdbpassword"));
				dataDictionary.setSourceTable(getOption("sourcetable"));
				dataDictionary.setSourceQuery(getOption("sourcequery"));
				
				dataDictionary.setTargetPropertyFile(getOption("trgconnaddpropertyfile"));
				dataDictionary.setTargetDatabaseDriver(getOption("trgdbdriverclass"));
				dataDictionary.setTargetConnectionURL(getOption("trgdbconnectionurl"));
				dataDictionary.setTargetUserName(getOption("trgdbusername"));
				dataDictionary.setTargetPassWord(getOption("trgdbpassword"));
				dataDictionary.setTargetTable(getOption("targettable"));
				dataDictionary.setTargetColumns(getOption("targetcolumns"));		
				
				String mappingDefFile = getOption("mapdeffile");
				dataDictionary.setMappingDefFile(mappingDefFile);
				
				try {
					if (mappingDefFile!=null) {
						dataDictionary.retrieveMappingDefinition();
					}
					dataDictionary.retrieveColumns();
					dataDictionary.executeInsert();
				}
				catch (Exception e) {
					logger.error("UNEXPECTED EXCEPTION");
					logger.error(e.getMessage());
				    throw e;
				}
	    	}*/
	    	if (function.equalsIgnoreCase("mergefiles")) {
				org.openbusinessintelligence.tools.file.FileMergeBean fileMerge = new org.openbusinessintelligence.tools.file.FileMergeBean();
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
		
		cmdOptions = new Options();		
		Option option = new Option("help", "Print this message");
		cmdOptions.addOption(option);
		
		org.w3c.dom.Document optionsXML = null;
		try {
			DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
			javax.xml.parsers.DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
			optionsXML = docBuilder.parse("cmd/toolsCmdOptions.xml");
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
