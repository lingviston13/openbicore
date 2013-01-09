package org.openbusinessintelligence.tools.db;

import java.io.*;
import java.sql.*;
import java.util.*;

import javax.naming.*;
import javax.sql.*;

/**
 * This class contains methods to execute procedures stored in a RDBMS
 * @author marangon
 */
public class ProcedureBean {

	private final static java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(ProcedureBean.class.getPackage().getName());

	// Declarations of bean properties
    private String propertyFile = "";
	private String databaseDriver = "";
	private String connectionURL = "";
	private String userName = "";
	private String passWord = "";
	private String sourceName = "";
	private String procedureName = "";
	private String[] procedureParameters = null;

	// Declarations of internally used variables
    private Properties properties = null;
	private Connection con = null;
	private CallableStatement callStmt = null;
	private String output = "Failure";

	/**
	 * Constructor
	 */
	public ProcedureBean() {
		super();
	}

    // Set source properties methods
    public void setPropertyFile(String pr) {
    	propertyFile = pr;
    }

	/**
	 * Set the database JDBC driver
	 */
	public void setDatabaseDriver(String dd) {
		databaseDriver = dd;
	}

	/**
	 * Set the database connection URL (used if no data sorce given)
	 */
	public void setConnectionURL(String cu) {
		connectionURL = cu;
	}

	/**
	 * Set the connection username (used if no data sorce given)
	 */
	public void setUserName(String un) {
		userName = un;
	}

	/**
	 * Set the connection password (used if no data sorce given)
	 */
	public void setPassWord(String pw) {
		passWord = pw;
	}

	/**
	 * Set the data source name
	 */
	public void setSourceName(String sn) {
		sourceName = sn;
	}

	/**
	 * Set the name of the procedure to execute
	 */
	public void setProcedureName(String pn) {
		procedureName = pn;
	}

	/**
	 * Set the parameters values in positional way
	 */
	public void setProcedureParameters(String[] pp) {
		procedureParameters = pp;
	}

	/**
	 * Get the eventual generated output
	 */
	public String getOutput() {
		return output;
	}

    public void openConnection() throws Exception  {
       	
    	DataSource ds = null;
    	
    	LOGGER.info("Opening source connection...");
    	
        if (sourceName == null ||sourceName.equals("")) {
        	Class.forName(databaseDriver).newInstance();
        	LOGGER.info("Loaded database driver " + databaseDriver);
        	if (propertyFile == null || propertyFile.equals("")) {
            	
            	LOGGER.info("Using username & password");
        		
            	con = DriverManager.getConnection(connectionURL, userName, passWord);
        	}
        	else {
            	
            	LOGGER.info("Using property file " + propertyFile);
        		
            	properties = new Properties();
        		properties.load(new FileInputStream(propertyFile));
        		con = DriverManager.getConnection(connectionURL, properties);
        	}
        	LOGGER.fine("Connected to database " + connectionURL);
        }
        else {
        	InitialContext ic;
        	ic = new InitialContext();
        	ds = (DataSource)ic.lookup("java:comp/env/jdbc/" + sourceName.toLowerCase());
        	con = ds.getConnection();
        	LOGGER.fine("Connected to database " + sourceName);
        }
        
    	LOGGER.info("Opened source connection");
    	
    }

    /**
	 * Execute the procedure
	 */
	public void execute() {

		try {
			callStmt = con.prepareCall("{call " + procedureName + "}");
			try {
				for (int i = 0; i < procedureParameters.length; i++) {
					System.out.println("Param "+ i + ": " + procedureParameters[i]);
					callStmt.setString(i + 1, procedureParameters[i]);
				}
			}
			catch(NullPointerException e) {
				System.out.println("No procedure parameters defined");
			}
			//Execute statement
			callStmt.execute();
			callStmt.close();
			con.close();
			output = "Success";
			System.out.println("Procedure executed.");

		}
		catch (SQLException e1) {
			System.out.println("Cannot execute procedure");
			e1.printStackTrace();
			try {
				con.close();
			}
			catch(SQLException e2) {
				e2.printStackTrace();
			}
		}
	}
}
