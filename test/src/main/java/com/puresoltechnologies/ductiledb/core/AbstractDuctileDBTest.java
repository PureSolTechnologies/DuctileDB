package com.puresoltechnologies.ductiledb.core;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.yaml.snakeyaml.Yaml;

import com.puresoltechnologies.ductiledb.api.DuctileDB;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;

public class AbstractDuctileDBTest {

    private static DuctileDBConfiguration configuration = null;
    private static DuctileDB ductileDB = null;

    /**
     * Initializes the database.
     * 
     * @throws MasterNotRunningException
     * @throws ZooKeeperConnectionException
     * @throws ServiceException
     * @throws IOException
     * @throws SchemaException
     * @throws StorageException
     */
    @BeforeClass
    public static void initializeDuctileDB() throws StorageException, SchemaException, IOException {
	configuration = readTestConfigration();
	ductileDB = createDuctileDB();
	assertNotNull("DuctileDB is null.", ductileDB);
    }

    public static DuctileDBConfiguration readTestConfigration() throws IOException {
	Yaml yaml = new Yaml();
	try (InputStream inputStream = AbstractDuctileDBTest.class.getResourceAsStream("/ductiledb-test.yml")) {
	    return yaml.loadAs(inputStream, DuctileDBConfiguration.class);
	}
    }

    public static DuctileDB createDuctileDB() throws StorageException, SchemaException {
	return DuctileDBFactory.connect(configuration);
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
	    if (ductileDB != null) {
		ductileDB.close();
	    }
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
