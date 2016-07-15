package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.File;
import java.io.IOException;

import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManager;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManagerImpl;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class StorageEngine {

    private final Storage storage;
    private final String storageName;
    private final File storageDirectory;
    private final SchemaManager schemaManager;

    public StorageEngine(Storage storage, String storageName) throws IOException {
	this.storage = storage;
	this.storageName = storageName;
	this.storageDirectory = new File(storageName);
	storage.createDirectory(storageDirectory);
	this.schemaManager = new SchemaManagerImpl(storage, storageDirectory);
    }

    Storage getStorage() {
	return storage;
    }

    public SchemaManager getSchemaManager() {
	return schemaManager;
    }

    public void store(byte[] key, byte[] value) {

    }

    public void delete(byte[] key) {

    }

}
