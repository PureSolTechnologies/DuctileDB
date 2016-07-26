package com.puresoltechnologies.ductiledb.storage.engine;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.yaml.snakeyaml.Yaml;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.api.StorageFactory;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;
import com.puresoltechnologies.ductiledb.stores.os.OSStorage;

public abstract class AbstractDatabaseEngineTest {

    private static DatabaseEngineConfiguration configuration;

    private DatabaseEngineImpl storageEngine;
    private String databaseEngineName;

    @BeforeClass
    public static void readConfiguration() throws IOException {
	Yaml yaml = new Yaml();
	try (InputStream inputStream = AbstractDatabaseEngineTest.class.getResourceAsStream("/database-engine.yml")) {
	    configuration = yaml.loadAs(inputStream, DatabaseEngineConfiguration.class);
	    assertNotNull(configuration);
	}
    }

    @Before
    public void initializeStorageEngine() throws StorageException, IOException {
	databaseEngineName = getClass().getSimpleName();
	storageEngine = new DatabaseEngineImpl(StorageFactory.getStorageInstance(configuration.getStorage()),
		databaseEngineName, configuration);
    }

    @After
    public void cleanupStorageEngine() throws IOException {
	if (storageEngine != null) {
	    Storage storage = storageEngine.getStorage();
	    storageEngine.close();
	    storage.removeDirectory(new File(databaseEngineName), true);
	}
	File baseDirectory = new File(
		configuration.getStorage().getProperties().getProperty(OSStorage.DIRECTORY_PROPERTY));
	baseDirectory.delete();
    }

    public static DatabaseEngineConfiguration getConfiguration() {
	return configuration;
    }

    public static void setConfiguration(DatabaseEngineConfiguration configuration) {
	AbstractDatabaseEngineTest.configuration = configuration;
    }

    protected DatabaseEngineImpl getEngine() {
	return storageEngine;
    }

}
