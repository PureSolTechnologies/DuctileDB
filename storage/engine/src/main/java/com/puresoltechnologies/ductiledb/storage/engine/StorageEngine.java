package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManager;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManagerImpl;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class StorageEngine implements Closeable {

    private boolean closed = false;
    private final Storage storage;
    private final File storageDirectory;
    private final SchemaManager schemaManager;

    public StorageEngine(Storage storage, String storageName) throws StorageException {
	this.storage = storage;
	this.storageDirectory = new File(storageName);
	try {
	    storage.createDirectory(storageDirectory);
	} catch (IOException e) {
	    throw new StorageException("Could not initialize storage engine.");
	}
	this.schemaManager = new SchemaManagerImpl(storage, storageDirectory);
    }

    @Override
    public void close() throws IOException {
	closed = true;
	storage.close();
    }

    public boolean isClosed() {
	return closed;
    }

    Storage getStorage() {
	return storage;
    }

    public SchemaManager getSchemaManager() {
	return schemaManager;
    }

    public Table getTable(String tableName) {
	// TODO Auto-generated method stub
	return null;
    }

}
