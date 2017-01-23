package com.puresoltechnologies.ductiledb.core;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.puresoltechnologies.ductiledb.logstore.utils.DefaultObjectMapper;
import com.puresoltechnologies.ductiledb.storage.api.StorageFactory;
import com.puresoltechnologies.ductiledb.storage.spi.FileStatus;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class AbstractDuctileDBTest {

    public static final URL DEFAULT_TEST_CONFIG_URL = AbstractDuctileDBTest.class.getResource("/ductiledb-test.json");

    private static DuctileDBConfiguration configuration = null;
    private static DuctileDB ductileDB = null;
    private static Storage storage = null;

    /**
     * Initializes the database.
     * 
     * @throws MasterNotRunningException
     * @throws ZooKeeperConnectionException
     * @throws ServiceException
     * @throws IOException
     * @throws SchemaException
     */
    @BeforeClass
    public static void initializeDuctileDB() throws IOException {
	configuration = readTestConfigration();
	storage = StorageFactory.getStorageInstance(configuration.getBigTableEngine().getStorage());
	cleanTestStorageDirectory(storage);
	if (configuration == null) {
	    configuration = readTestConfigration();
	}
	startDatabase();
	assertNotNull("DuctileDB is null.", ductileDB);
    }

    protected static void startDatabase() {
	DuctileDBBootstrap.start(configuration);
	ductileDB = DuctileDBBootstrap.getInstance();
    }

    public static void cleanTestStorageDirectory(Storage storage) throws FileNotFoundException, IOException {
	for (File file : storage.list(new File("/"))) {
	    FileStatus fileStatus = storage.getFileStatus(file);
	    if (fileStatus.isDirectory()) {
		storage.removeDirectory(file, true);
	    } else {
		storage.delete(file);
	    }
	}
    }

    public static DuctileDBConfiguration readTestConfigration() throws IOException {
	ObjectMapper objectMapper = DefaultObjectMapper.getInstance();
	try (InputStream inputStream = DEFAULT_TEST_CONFIG_URL.openStream()) {
	    return objectMapper.readValue(inputStream, DuctileDBConfiguration.class);
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
