package org.openbusinessintelligence.core;

import static org.junit.Assert.*;

import org.junit.Test;

public class MainTestDBPropertiesNoPropertyFile {
	
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
		arguments[5] = "jdbc:mysql://localhost:3306/sugarcrm";
		arguments[7] = "sugarcrm";
		arguments[9] = "sugarcrm";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
}
