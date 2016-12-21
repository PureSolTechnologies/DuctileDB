package com.puresoltechnologies.ductiledb.backend;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;

import org.junit.Test;

public class PostgreSQLIT extends AbstractBackendTest {

    @Test
    public void testConnectionSettings() throws Exception {
	PostgreSQL postgreSQL = getDatabase();
	try (Connection connection = postgreSQL.getConnection()) {
	    assertNotNull("No connection returned.", connection);
	    assertFalse(connection.getAutoCommit());
	}
    }

    @Test
    public void testMetaData() throws Exception {
	PostgreSQL postgreSQL = getDatabase();
	try (Connection connection = postgreSQL.getConnection()) {
	    assertNotNull("No connection returned.", connection);
	    DatabaseMetaData metaData = connection.getMetaData();
	    assertTrue(metaData.supportsTransactions());
	}
    }

    @Test
    public void testPoolSimple() throws Exception {
	PostgreSQL postgreSQL = getDatabase();
	try (Connection connection1 = postgreSQL.getConnection()) {
	    assertNotNull("No connection returned.", connection1);
	    try (Connection connection2 = postgreSQL.getConnection()) {
		assertNotNull("No connection returned.", connection2);
		assertNotSame("No connection returned.", connection1, connection2);
	    }
	}
    }

}
