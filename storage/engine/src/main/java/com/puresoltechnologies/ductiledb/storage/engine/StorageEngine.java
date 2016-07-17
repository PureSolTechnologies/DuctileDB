package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManager;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManagerImpl;
import com.puresoltechnologies.ductiledb.storage.engine.schema.TableDescriptor;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class StorageEngine implements Closeable {

    private boolean closed = false;
    private final Storage storage;
    private final String storageName;
    private final File storageDirectory;
    private final SchemaManager schemaManager;

    public StorageEngine(Storage storage, String storageName) throws StorageException {
	this.storage = storage;
	this.storageName = storageName;
	this.storageDirectory = new File(storageName);
	try {
	    storage.createDirectory(storageDirectory);
	} catch (IOException e) {
	    throw new StorageException("Could not initialize storage engine.");
	}
	this.schemaManager = new SchemaManagerImpl(this, storageDirectory);
    }

    @Override
    public void close() throws IOException {
	closed = true;
	storage.close();
    }

    public final boolean isClosed() {
	return closed;
    }

    public final Storage getStorage() {
	return storage;
    }

    public final String getStoreName() {
	return storageName;
    }

    public final SchemaManager getSchemaManager() {
	return schemaManager;
    }

    public Table getTable(String tableName) {
	TableDescriptor tableDescriptor = schemaManager.getTable(tableName);
	return getTable(tableDescriptor);
    }

    public Table getTable(TableDescriptor tableDescriptor) {
	return new Table(this, tableDescriptor);
    }

    public Map<String, LogEntry> read(TableDescriptor tableDescriptor, byte[] key, Set<String> columnFamilies) {
	// TODO Auto-generated method stub
	return null;
    }

    public void store(TableDescriptor tableDescriptor, byte[] key, Map<String, LogEntry> columns) {
	// TODO Auto-generated method stub
    }

    public void delete(TableDescriptor tableDescriptor, byte[] key, Set<String> columnFamilies) {
	// TODO Auto-generated method stub

    }

}
