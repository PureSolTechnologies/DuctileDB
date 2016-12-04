package com.puresoltechnologies.ductiledb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.puresoltechnologies.ductiledb.core.utils.BuildInformation;
import com.puresoltechnologies.versioning.Version;

public class DuctileDatabaseMetaDataIT extends AbstractJDBCTest {

    private static Connection connection;
    private static DatabaseMetaData metaData;

    @BeforeClass
    public static void readMetaData() throws SQLException {
	metaData = connection.getMetaData();
    }

    @Test
    public void testDatabaseProductInformation() throws SQLException {
	assertEquals("Ductile Database", metaData.getDatabaseProductName());
	Version version = Version.valueOf(BuildInformation.getVersion());
	assertEquals(version.toString(), metaData.getDatabaseProductVersion());
	assertEquals(version.getMajor(), metaData.getDatabaseMajorVersion());
	assertEquals(version.getMinor(), metaData.getDatabaseMinorVersion());
    }

    @Test
    public void testDriverInformation() throws SQLException {
	assertEquals(DuctileDriver.class.getName(), metaData.getDriverName());
	Version version = Version.valueOf(BuildInformation.getVersion());
	assertEquals(version.toString(), metaData.getDriverVersion());
	assertEquals(version.getMajor(), metaData.getDriverMajorVersion());
	assertEquals(version.getMinor(), metaData.getDriverMinorVersion());
    }

    @Test
    public void testKeywordsAndFunctions() throws SQLException {
	assertEquals("", metaData.getSQLKeywords());
	assertEquals("", metaData.getNumericFunctions());
	assertEquals("", metaData.getStringFunctions());
	assertEquals("", metaData.getSystemFunctions());
	assertEquals("", metaData.getTimeDateFunctions());
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
