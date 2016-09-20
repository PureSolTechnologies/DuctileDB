package com.puresoltechnologies.ductiledb.core;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.yaml.snakeyaml.Yaml;

import com.puresoltechnologies.ductiledb.api.DuctileDB;
import com.puresoltechnologies.ductiledb.storage.api.StorageFactory;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.storage.spi.FileStatus;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class AbstractDuctileDBTest {

    public static final URL DEFAULT_TEST_CONFIG_URL = AbstractDuctileDBTest.class.getResource("/ductiledb-test.yml");

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
    public static void initializeDuctileDB() throws SchemaException, IOException {
	configuration = readTestConfigration();
	storage = StorageFactory.getStorageInstance(configuration.getDatabaseEngine().getStorage());
	cleanTestStorageDirectory(storage);
	ductileDB = createDuctileDB();
	assertNotNull("DuctileDB is null.", ductileDB);
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
	Yaml yaml = new Yaml();
	try (InputStream inputStream = DEFAULT_TEST_CONFIG_URL.openStream()) {
	    return yaml.loadAs(inputStream, DuctileDBConfiguration.class);
	}
    }

    public static DuctileDB createDuctileDB() throws SchemaException, IOException {
	if (configuration == null) {
	    configuration = readTestConfigration();
	}
	DuctileDBBootstrap.start(configuration);
	return DuctileDBBootstrap.getInstance();
    }

    /**
     * Shuts down DuctileDB after test.
     * 
     * @throws IOException
     *             is thrown in case of I/O issues.
     */
    @AfterClass
    public static void shutdownDuctileDB() throws IOException {
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
