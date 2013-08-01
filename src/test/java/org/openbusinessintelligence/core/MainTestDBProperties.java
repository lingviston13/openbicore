package org.openbusinessintelligence.core;

import static org.junit.Assert.*;

import org.junit.Test;

public class MainTestDBProperties {
	
	private String[] arguments = new String[10];
	
	public void initArguments() {
		
		// Function to test
		arguments[0] = "-function";
		arguments[1] = "dbproperties";
		// Mandatory arguments
		arguments[2] = "-dbdriverclass";
		arguments[4] = "-dbconnectionurl";
		arguments[6] = "-dbusername";
		arguments[8] = "-dbpassword";
		
	}

	@Test
	public void testMySQL() {
		
		initArguments();
		//
		arguments[3] = "com.mysql.jdbc.Driver";
		arguments[5] = "jdbc:mysql://localhost:3306/dwhstage";
		arguments[7] = "dwhstage";
		arguments[9] = "dwhstage";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testPostgreSQL() {
		
		initArguments();
		//
		arguments[3] = "org.postgresql.Driver";
		arguments[5] = "jdbc:postgresql://localhost:5432/postgres";
		arguments[7] = "dwhload";
		arguments[9] = "dwhload";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testSQLServer() {
		
		initArguments();
		//
		arguments[3] = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		arguments[5] = "jdbc:sqlserver://localhost:1433;instance=MSSQLSERVER";
		arguments[7] = "dwhadmin";
		arguments[9] = "openbi";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testDB2() {
		
		initArguments();
		//
		arguments[3] = "com.ibm.db2.jcc.DB2Driver";
		arguments[5] = "jdbc:db2://localhost:50000/SAMPLE";
		arguments[7] = "db2user";
		arguments[9] = "db2user";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testOracle() {
		
		initArguments();
		//
		arguments[3] = "oracle.jdbc.OracleDriver";
		arguments[5] = "jdbc:oracle:thin:@//localhost:1521/dwhdev";
		arguments[7] = "dwhstage";
		arguments[9] = "dwhstage";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
}
