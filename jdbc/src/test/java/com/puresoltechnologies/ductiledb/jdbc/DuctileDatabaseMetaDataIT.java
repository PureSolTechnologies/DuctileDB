package com.puresoltechnologies.ductiledb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DuctileDatabaseMetaDataIT extends AbstractJDBCTest {

    private static Connection connection;
    private static DatabaseMetaData metaData;

    @BeforeClass
    public static void connect() throws SQLException {
	connection = DriverManager.getConnection(
		"jdbc:ductile:file:" + new File("src/test/resources/ductiledb-test.yml").getAbsolutePath());
	assertEquals(DuctileConnection.class, connection.getClass());

	metaData = connection.getMetaData();
    }

    @AfterClass
    public static void disconnect() throws SQLException {
	connection.close();
    }

    @Test
    public void testGetCatalogs() throws SQLException {
	ResultSet catalogs = metaData.getCatalogs();
	assertTrue(catalogs.next());
	boolean foundSystem = false;
	do {
	    String catalog = catalogs.getString("TABLE_CAT");
	    assertNotNull(catalog);
	    if ("system".equals(catalog)) {
		foundSystem = true;
	    }
	} while (catalogs.next());
	assertTrue(foundSystem);
    }

}
