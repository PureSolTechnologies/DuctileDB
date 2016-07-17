package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.api.StorageFactory;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;
import com.puresoltechnologies.ductiledb.stores.os.OSStorage;

public abstract class AbstractStorageEngineTest {

    private static final UUID randomUUID = UUID.randomUUID();
    private static final File baseDirectory = new File("/tmp/storage");
    private static final File storageDirectory = new File(baseDirectory, randomUUID.toString());
    private static StorageEngine storageEngine;

    @BeforeClass
    public static void initialize() throws StorageException {
	HashMap<String, String> configuration = new HashMap<>();
	configuration.put(OSStorage.DIRECTORY_PROPERTY, storageDirectory.getPath());
	storageEngine = new StorageEngine(StorageFactory.getStorageInstance(configuration), "test");
    }

    @AfterClass
    public static void cleanup() throws IOException {
	Storage storage = storageEngine.getStorage();
	storage.removeDirectory(new File("test"), true);
	storageDirectory.delete();
	baseDirectory.delete();
    }

    protected StorageEngine getEngine() {
	return storageEngine;
    }

}
