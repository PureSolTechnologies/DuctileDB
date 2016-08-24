package com.puresoltechnologies.ductiledb.core;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.puresoltechnologies.ductiledb.api.DuctileDB;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.api.StorageFactory;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.storage.spi.FileStatus;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This test assures an empty storage directory and starts, stops, starts and
 * stops again DuctileDB to assure consistency in directories.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class DuctileDBStartStopStartStopIT {
    private static DuctileDBConfiguration configuration = null;
    private static Storage storage = null;

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
	configuration = AbstractDuctileDBTest.readTestConfigration();
	assertNotNull("No configuration loaded.", configuration);
	storage = StorageFactory.getStorageInstance(configuration.getDatabaseEngine().getStorage());
	cleanTestStorageDirectory();
    }

    private static void cleanTestStorageDirectory() throws FileNotFoundException, IOException {
	for (File file : storage.list(new File("/"))) {
	    FileStatus fileStatus = storage.getFileStatus(file);
	    if (fileStatus.isDirectory()) {
		storage.removeDirectory(file, true);
	    } else {
		storage.delete(file);
	    }
	}
    }

    @Test
    public void test() throws StorageException, SchemaException, IOException {
	System.out.println("===== 1st start of database =====");
	DuctileDBBootstrap.start(configuration);
	DuctileDB ductileDB1 = DuctileDBBootstrap.getInstance();
	assertNotNull("DuctileDB is null.", ductileDB1);
	DuctileDBBootstrap.stop();

	System.out.println("===== 2nd start of database =====");
	DuctileDBBootstrap.start(configuration);
	DuctileDB ductileDB2 = DuctileDBBootstrap.getInstance();
	assertNotNull("DuctileDB is null.", ductileDB2);
	DuctileDBBootstrap.stop();
    }

}
