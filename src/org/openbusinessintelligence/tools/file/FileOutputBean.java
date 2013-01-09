package org.openbusinessintelligence.tools.file;

import java.io.*;
import java.sql.*;
import javax.sql.rowset.*;

/**
 * Utility class to facilitate the use of files as output targets
 * @author marangon
 */
public class FileOutputBean {

	private final static java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(FileOutputBean.class.getPackage().getName());

	private WebRowSet webRS;
	private String contentString;
	private String directoryName = "";
	private String fileName;
	private Writer writer;

	public FileOutputBean() {
		super();
	}

	// Set property methods
	public void setDirectoryName(String property) {
		directoryName = property;
	}

	public void setFileName(String property) {
		fileName = property;
	}

	public void setWebRS(WebRowSet property) {
		webRS = property;
	}

	public void setContentString(String property) {
		contentString = property;
	}

	// Instantiate the writer object
	public Writer getWriter() throws Exception {
		File outputFile = null;
		if (directoryName.equals("")) {
			outputFile = new File(fileName);
		}
		else {
			outputFile = new File(directoryName + File.separatorChar + fileName);
		}
		try {
			writer = new java.io.FileWriter(outputFile);
		}
		catch (IOException e) {
			System.out.println("Cannot create output file" + directoryName + File.separatorChar + fileName);
			System.out.println(e);
			throw e;
		}
		return writer;
	}

	// Write content
	public void writeContentString() throws Exception {
		File outputFile = null;
		if (directoryName.equals("")) {
			outputFile = new File(fileName);
		}
		else {
			outputFile = new File(directoryName + File.separatorChar + fileName);
		}
		try {
			writer = new java.io.FileWriter(outputFile);
			BufferedWriter buffWriter = new BufferedWriter(writer);
			buffWriter.write(contentString);
			buffWriter.close();
		}
		catch (IOException e) {
			System.out.println("Cannot create output file" + directoryName + File.separatorChar + fileName);
			System.out.println(e);
			throw e;
		}
	}

	public void writeWRS() throws Exception {
		File outputFile = null;
		if (directoryName.equals("")) {
			outputFile = new File(fileName);
		}
		else {
			outputFile = new File(directoryName + File.separatorChar + fileName);
		}
		try {
			outputFile = new File(directoryName + File.separatorChar + fileName);
			writer = new java.io.FileWriter(outputFile);
		}
		catch (IOException e) {
			System.out.println("Cannot create output file" + directoryName + File.separatorChar + fileName);
			System.out.println(e);
			throw e;
		}
		try {
			webRS.writeXml(writer);
		}
		catch (SQLException e) {
			System.out.println("Cannot write output to file" + directoryName + File.separatorChar + fileName);
			System.out.println(e);
			throw e;
		}
	}
}
