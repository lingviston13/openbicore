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
		arguments[5] = "jdbc:mysql://msas4042i.msg.de:3306/dwh_extract";
		arguments[7] = "dwh_extract";
		arguments[9] = "msg2013";
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
		arguments[5] = "jdbc:postgresql://msas4042i.msg.de:5432/postgres";
		arguments[7] = "dwhextract";
		arguments[9] = "msg2013";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

}
