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

public abstract class AbstractStorageEngineTest {

    private static DatabaseEngineConfiguration configuration;

    private DatabaseEngine storageEngine;

    @BeforeClass
    public static void readConfiguration() throws IOException {
	Yaml yaml = new Yaml();
	try (InputStream inputStream = AbstractStorageEngineTest.class.getResourceAsStream("/database-engine.yml")) {
	    configuration = yaml.loadAs(inputStream, DatabaseEngineConfiguration.class);
	    assertNotNull(configuration);
	}
    }

    @Before
    public void initializeStorageEngine() throws StorageException {
	storageEngine = new DatabaseEngine(StorageFactory.getStorageInstance(configuration.getStorage()), "test");
    }

    @After
    public void cleanupStorageEngine() throws IOException {
	Storage storage = storageEngine.getStorage();
	storage.removeDirectory(new File("test"), true);
	File baseDirectory = new File(
		configuration.getStorage().getProperties().getProperty(OSStorage.DIRECTORY_PROPERTY));
	File storageDirectory = new File(baseDirectory, "test");
	storageDirectory.delete();
	baseDirectory.delete();
    }

    public static DatabaseEngineConfiguration getConfiguration() {
	return configuration;
    }

    public static void setConfiguration(DatabaseEngineConfiguration configuration) {
	AbstractStorageEngineTest.configuration = configuration;
    }

    protected DatabaseEngine getEngine() {
	return storageEngine;
    }

}
