package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.puresoltechnologies.ductiledb.storage.spi.Storage;
import com.puresoltechnologies.ductiledb.stores.os.OSStorage;

public abstract class AbstractStorageEngineTest {

    private static final UUID randomUUID = UUID.randomUUID();
    private static final String directory = "/tmp/storage/" + randomUUID.toString();
    private static StorageEngine storageEngine;

    @BeforeClass
    public static void initialize() throws IOException {
	HashMap<String, String> configuration = new HashMap<>();
	configuration.put(OSStorage.DIRECTORY_PROPERTY, directory);
	storageEngine = EngineFactory.create(configuration, "test");

    }

    @AfterClass
    public static void cleanup() throws FileNotFoundException, IOException {
	Storage storage = storageEngine.getStorage();
	storage.removeDirectory(new File("test"), true);
    }

    protected StorageEngine getEngine() {
	return storageEngine;
    }

}
