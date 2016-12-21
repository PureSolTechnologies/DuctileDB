package com.puresoltechnologies.ductiledb.blobstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.yaml.snakeyaml.Yaml;

import com.puresoltechnologies.ductiledb.backend.DatabaseConfiguration;
import com.puresoltechnologies.ductiledb.backend.PostgreSQL;

public abstract class AbstractBlobStoreTest {

    private static DatabaseConfiguration databaseConfiguration = null;
    private static PostgreSQL connection = null;

    @BeforeClass
    public static void initialize() throws IOException, SQLException {
	databaseConfiguration = readDatabaseConfiguration();
	connect();
    }

    protected static DatabaseConfiguration readDatabaseConfiguration() throws IOException {
	try (InputStream databaseConfigurationStream = AbstractBlobStoreTest.class
		.getResourceAsStream("/database.yml")) {
	    assertNotNull("Test configuration could not be found.", databaseConfigurationStream);
	    Yaml yaml = new Yaml();
	    DatabaseConfiguration databaseConfiguration = yaml.loadAs(databaseConfigurationStream,
		    DatabaseConfiguration.class);
	    assertNotNull("Configuration could not be read.", databaseConfiguration);
	    assertEquals("jdbc:postgresql://localhost:5432/ductiledb?user=ductiledb&password=ductiledb",
		    databaseConfiguration.getJdbcUrl());
	    return databaseConfiguration;
	}

    }

    protected static BlobStoreConfiguration readBlobStoreConfiguration() throws IOException {
	try (InputStream blobStoreConfigurationStream = AbstractBlobStoreTest.class
		.getResourceAsStream("/blobstore.yml")) {
	    assertNotNull("Could not find test configuration.", blobStoreConfigurationStream);
	    Yaml yaml = new Yaml();
	    BlobStoreConfiguration blobStoreConfiguration = yaml.loadAs(blobStoreConfigurationStream,
		    BlobStoreConfiguration.class);
	    assertNotNull("Could not read configuration.", blobStoreConfiguration);
	    assertEquals(1048576, blobStoreConfiguration.getMaxFileSize());
	    assertEquals(65536, blobStoreConfiguration.getChunkSize());
	    return blobStoreConfiguration;
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

    protected static PostgreSQL getConnection() {
	return connection;
    }
}
