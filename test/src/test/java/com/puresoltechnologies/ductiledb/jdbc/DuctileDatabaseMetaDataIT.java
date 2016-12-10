package com.puresoltechnologies.ductiledb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

import com.puresoltechnologies.ductiledb.core.utils.BuildInformation;
import com.puresoltechnologies.versioning.Version;

public class DuctileDatabaseMetaDataIT extends AbstractJDBCTest {
    private static DatabaseMetaData metaData;

    @Before
    public void readMetaData() throws SQLException {
	metaData = getConnection().getMetaData();
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
	int count = 0;
	boolean foundTableStore = false;
	boolean foundBlobStore = false;
	boolean foundGraphStore = false;
	do {
	    String catalog = catalogs.getString("TABLE_CAT");
	    assertNotNull(catalog);
	    if ("table_store".equals(catalog)) {
		foundTableStore = true;
	    }
	    if ("blob_store".equals(catalog)) {
		foundBlobStore = true;
	    }
	    if ("graph_store".equals(catalog)) {
		foundGraphStore = true;
	    }
	    count++;
	} while (catalogs.next());
	assertEquals(3, count);
	assertTrue(foundTableStore);
	assertTrue(foundBlobStore);
	assertTrue(foundGraphStore);
    }

}
