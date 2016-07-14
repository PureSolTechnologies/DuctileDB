package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.puresoltechnologies.ductiledb.storage.spi.Storage;
import com.puresoltechnologies.ductiledb.stores.os.OSStorage;

public class EngineIT {

    private static final UUID randomUUID = UUID.randomUUID();
    private static final String directory = "/tmp/storage/" + randomUUID.toString();
    private static StorageEngine engine;

    @BeforeClass
    public static void initialize() throws IOException {
	HashMap<String, String> configuration = new HashMap<>();
	configuration.put(OSStorage.DIRECTORY_PROPERTY, directory);
	engine = EngineFactory.create(configuration, "test");

    }

    @AfterClass
    public static void cleanup() throws FileNotFoundException, IOException {
	Storage storage = engine.getStorage();
	storage.removeDirectory(new File("test"), true);
    }

    @Test
    public void test() throws IOException {
    }

}
