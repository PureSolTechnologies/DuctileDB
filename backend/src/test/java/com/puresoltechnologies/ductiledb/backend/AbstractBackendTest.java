package com.puresoltechnologies.ductiledb.backend;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.yaml.snakeyaml.Yaml;

public abstract class AbstractBackendTest {

    private static DatabaseConfiguration databaseConfiguration = null;
    private static PostgreSQL connection = null;

    @BeforeClass
    public static void initialize() throws IOException, SQLException {
	databaseConfiguration = readDatabaseConfiguration();
	connect();
    }

    protected static DatabaseConfiguration readDatabaseConfiguration() throws IOException {
	try (InputStream databaseConfigurationStream = AbstractBackendTest.class.getResourceAsStream("/database.yml")) {
	    assertNotNull("Test configuration could not be found.", databaseConfigurationStream);
	    Yaml yaml = new Yaml();
	    DatabaseConfiguration databaseConfiguration = yaml.loadAs(databaseConfigurationStream,
		    DatabaseConfiguration.class);
	    assertNotNull("Configuration could not be read.", databaseConfiguration);
	    assertEquals("localhost", databaseConfiguration.getHost());
	    assertEquals(5432, databaseConfiguration.getPort());
	    assertEquals("ductiledb", databaseConfiguration.getDatabase());
	    assertEquals("ductiledb", databaseConfiguration.getUsername());
	    assertEquals("ductiledb", databaseConfiguration.getPassword());
	    assertEquals("jdbc:postgresql://localhost:5432/ductiledb?user=ductiledb&password=ductiledb",
		    databaseConfiguration.getJdbcUrl());
	    return databaseConfiguration;
	}

    }

    private static void connect() throws SQLException {
	PostgreSQL.connect(getDatabaseConfiguration());
	connection = PostgreSQL.get();
	assertNotNull("Connection could not be established.", connection);
    }

    @AfterClass
    public static void destroy() throws IOException, SQLException {
	disconnect();
    }

    private static void disconnect() throws IOException {
	PostgreSQL.disconnect();
    }

    protected static DatabaseConfiguration getDatabaseConfiguration() {
	return databaseConfiguration;
    }

    protected static PostgreSQL getDatabase() {
	return connection;
    }
}
