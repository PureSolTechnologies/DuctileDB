package com.puresoltechnologies.ductiledb.core;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.yaml.snakeyaml.Yaml;

public class AbstractDuctileDBTest {

    public static final String DEFAULT_TEST_CONFIG_URL = "ductiledb-test.yml";

    private static DuctileDBConfiguration configuration = null;
    private static DuctileDB ductileDB = null;

    /**
     * Initializes the database.
     * 
     * @throws IOException
     * @throws SQLException
     */
    @BeforeClass
    public static void initializeDuctileDB() throws IOException, SQLException {
	configuration = readTestConfigration();
	startDatabase();
	assertNotNull("DuctileDB is null.", ductileDB);
    }

    protected static void startDatabase() throws SQLException {
	DuctileDBBootstrap.start(configuration);
	ductileDB = DuctileDBBootstrap.getInstance();
    }

    public static DuctileDBConfiguration readTestConfigration() throws IOException {
	Yaml yaml = new Yaml();
	try (InputStream inputStream = AbstractDuctileDBTest.class.getResourceAsStream(DEFAULT_TEST_CONFIG_URL)) {
	    assertNotNull("Test configuration not found.", inputStream);
	    return yaml.loadAs(inputStream, DuctileDBConfiguration.class);
	}
    }

    /**
     * Shuts down DuctileDB after test.
     * 
     * @throws IOException
     *             is thrown in case of I/O issues.
     */
    @AfterClass
    public static void shutdownDuctileDB() throws IOException {
	stopDatabase();
    }

    protected static void stopDatabase() {
	try {
	    DuctileDBBootstrap.stop();
	} finally {
	    ductileDB = null;
	}
    }

    /**
     * Returns the initialized and connected database.
     * 
     * @return A {@link DuctileDB} object is returned, ready for use.
     */
    protected static DuctileDB getDuctileDB() {
	return ductileDB;
    }
}
