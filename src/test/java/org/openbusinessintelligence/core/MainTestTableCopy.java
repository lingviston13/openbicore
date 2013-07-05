package org.openbusinessintelligence.core;

import static org.junit.Assert.*;

import org.junit.Test;

public class MainTestTableCopy {
	
	private String[] arguments = new String[24];
	
	public void initArguments() {
		
		// Function to test
		arguments[0] = "-function";
		arguments[1] = "tablecopy";
		// Mandatory arguments
		arguments[2] = "-srcdbdriverclass";
		arguments[4] = "-srcdbconnectionurl";
		arguments[6] = "-srcdbusername";
		arguments[8] = "-srcdbpassword";
		arguments[10] = "-sourcetable";
		arguments[12] = "-trgdbdriverclass";
		arguments[14] = "-trgdbconnectionurl";
		arguments[16] = "-trgdbusername";
		arguments[18] = "-trgdbpassword";
		arguments[20] = "-targettable";
		arguments[22] = "-trgcreate";
		
	}

	@Test
	public void testMySQLtoPostgreSQL() {
		
		initArguments();
		//
		arguments[3] = "com.mysql.jdbc.Driver";
		arguments[5] = "jdbc:mysql://msas4042i.msg.de:3306/sugarcrm?transformedBitIsBoolean=false&tinyInt1isBit=false";
		arguments[7] = "dwh_extract";
		arguments[9] = "msg2013";
		arguments[11] = "emails";
		arguments[13] = "org.postgresql.Driver";
		arguments[15] = "jdbc:postgresql://msas4042i.msg.de:5432/postgres";
		arguments[17] = "dwh_stage";
		arguments[19] = "msg2013";
		arguments[21] = "stg_emails";
		arguments[23] = "true";
		// Perform test
		try {
			Main.main(arguments);
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}
}
