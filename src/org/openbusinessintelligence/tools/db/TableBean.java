package org.openbusinessintelligence.tools.db;

import java.sql.*;
import com.sun.rowset.*;

/**
 * This class contains methods to perform DMLs (inserts, updates, deletes) on a RDBMS table.
 * @author marangon
 */
public class TableBean {

	private final static java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(TableBean.class.getPackage().getName());

	// Declarations of bean properties
	private String databaseDriver = "";
	private String connectionURL = "";
	private String userName = "";
	private String passWord = "";
	private String sourceName = "";
	private String tableName = "";
	private String[] keyColumnNames = null;
	private String[] keyValues = null;
	private String[] columnNames = null;
	private String[] insertValues = null;

	// Declarations of internally used variables
	private WebRowSetImpl webRS;
	
	/**
	 * Open the connection and initialize the webrowset object
	 */
	private void open() {
		if (sourceName.equals("") || sourceName == null) {
			try {
				Class.forName(databaseDriver).newInstance();
				System.out.println("Loaded database driver " + databaseDriver);
			}
			catch (Exception e){
				System.out.println("Cannot load database driver " + databaseDriver);
				e.printStackTrace();
			}
			try {
				webRS = new WebRowSetImpl();
				webRS.setUrl(connectionURL);
				webRS.setUsername(userName);
				webRS.setPassword(passWord);
			}	
			catch(SQLException e) {
				System.out.println( "Cannot connect to datasource " + sourceName);
				e.printStackTrace();
			}
		}
		else {
			try {
				webRS = new WebRowSetImpl();
				webRS.setDataSourceName("java:comp/env/jdbc/" + sourceName.toLowerCase());
			}
			catch(SQLException e) {
				System.out.println( "Cannot connect to datasource " + sourceName);
				e.printStackTrace();
			}
		}
	}

	/**
	 * Constructor
	 */
	public TableBean() {
		super();
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
	 * Set the name of the table on which DML instructions are to be performed
	 */
	public void setTableName(String tn) {
		tableName = tn;
	}

	/**
	 * Set the name of the key columns (for updates and deletes)
	 */
	public void setKeyColumnNames(String[] cn) {
		keyColumnNames = cn;
	}

	/**
	 * Set the key values (for updates and deletes)
	 */
	public void setKeyValues(String[] cn) {
		keyValues = cn;
	}

	/**
	 * Set the column names on which DMLs are to be performed
	 */
	public void setColumnNames(String[] cn) {
		columnNames = cn;
	}

	/**
	 * Set values for performing DMLs
	 */
	public void setInsertValues(String[] iv) {
		insertValues = iv;
	}

	/**
	 * Perform an insert on the given table
	 */
	public void insert() {
		open();
		try {
			String sqlText = "SELECT ";
			for (int i = 0; i < columnNames.length; i++) {
				if (i > 0) {
					sqlText += ",";
				}
				sqlText += columnNames[i];
			}
			sqlText += " FROM " + tableName;
			webRS.setCommand(sqlText);
			webRS.execute();
			webRS.moveToInsertRow();
			try {
				for (int i = 0; i < insertValues.length; i++) {
					System.out.println("Param "+ i + ": " + insertValues[i]);
					webRS.updateObject(i + 1, insertValues[i]);
				}
			}
			catch(NullPointerException e) {
				System.out.println("No insert values defined");
			}
			webRS.insertRow();
			System.out.println("Values inserted.");
			webRS.moveToCurrentRow();
			webRS.acceptChanges();
			System.out.println("Commit performed.");

		}
		catch (SQLException e) {
			System.out.println("Cannot perform insert");
			e.printStackTrace();
		}
	}
	
	/**
	 * Perform an update on the given table
	 */
	public void update() {
		open();
	}
	
	/**
	 * Perform an delete on the given table
	 */
	public void delete() {
		open();
		
	}
}
