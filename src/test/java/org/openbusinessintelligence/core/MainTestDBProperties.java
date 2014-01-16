package org.openbusinessintelligence.core;

import static org.junit.Assert.*;

import org.junit.Test;

public class MainTestDBProperties {
	
	private String[] arguments = new String[4];
	
	public void initArguments() {
		
		// Function to test
		arguments[0] = "-function";
		arguments[1] = "dbproperties";
		// Mandatory arguments
		arguments[2] = "-dbconnpropertyfile";
		
	}

	/*@Test
	public void testMySQL() {
		
		initArguments();
		//
		arguments[3] = "mysql_localhost_sugarcrm";
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
		arguments[3] = "postgresql_localhost_postgres_sugarcrm";
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
		arguments[3] = "sqlserver_localhost_sugarcrm";
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
		arguments[3] = "db2_localhost_sugarcrm";
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
		arguments[3] = "oracle_localhost_sugarcrm";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}*/

	@Test
	public void testInformix() {
		
		initArguments();
		//
		arguments[3] = "informix_localhost_sugarcrm";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

	@Test
	public void testHANA() {
		
		initArguments();
		//
		arguments[3] = "hana_msas120i_sugarcrm";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
}
