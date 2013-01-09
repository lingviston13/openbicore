package org.openbusinessintelligence.tools.file;

import java.io.*;

/**
 * Utility class to facilitate the use of files as input sources
 * @author marangon
 */

public class FileInputBean {

	private final static java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(FileInputBean.class.getPackage().getName());

    // Declarations of bean properties
	private String directoryName = "";
	private String fileName = "";
	private String filePath = "";
	private String fileContent = "";
	private BufferedReader reader;
	private File file = null;

    // Constructor
	public FileInputBean() {
		super();
	}

    // Set properties methods
	public void setDirectoryName(String property) {
		directoryName = property;
	}

	public void setFileName(String property) {
		fileName = property;
	}

	public void setFilePath(String property) {
		filePath = property;
	}

    // Execution methods
	public BufferedReader getReader() throws Exception {
		createFile();
		FileReader fileReader = null;
		try {
			fileReader = new FileReader(file);
			reader =new BufferedReader(fileReader);
		}
		catch (Exception e) {
			LOGGER.severe("File " + filePath + " not found:\n" + e.getMessage());
			throw e;
		}
		return reader;
	}

	public String getString() throws Exception {
		createFile();
		FileReader fileReader = null;
		BufferedReader bufferedReader = null;
		try {
			fileReader = new FileReader(file);
			bufferedReader =new BufferedReader(fileReader);
		}
		catch (Exception e) {
			LOGGER.severe("File " + filePath + " not found:\n" + e.getMessage());
			throw e;
		}
		String lineBuffer;
		fileContent ="";
		try {
			while ((lineBuffer=bufferedReader.readLine()) != null) {
				fileContent += lineBuffer + "\n";
			}
		}
		catch (Exception e) {
			LOGGER.severe("File IO error:\n" + e.getMessage());
			throw e;
		}
		return fileContent;
	}
	
	private void createFile() {
		if (filePath.equals("")) {
			if (directoryName.equals("")) {
				filePath = fileName;
			}
			else {
				filePath = directoryName + File.separatorChar + fileName;
			}
		}
		file = new File(filePath);
	}
}
