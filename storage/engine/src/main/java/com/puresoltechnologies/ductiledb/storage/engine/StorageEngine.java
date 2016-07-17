package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
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

    public Map<String, LogEntry> read(TableDescriptor tableDescriptor, Get get) {
	// TODO Auto-generated method stub
	return null;
    }

    public void store(TableDescriptor tableDescriptor, Put put) {
	byte[] key = put.getKey();
	Instant timestamp = put.getTimestamp();
	Iterator<String> columnFamilies = put.getColumnFamilies();
	while (columnFamilies.hasNext()) {
	    String columnFamily = columnFamilies.next();
	    Map<byte[], byte[]> columnValues = put.getColumnValues(columnFamily);
	    store(tableDescriptor.getColumnFamily(columnFamily), key, timestamp, columnValues);
	}
    }

    private void store(ColumnFamilyDescriptor columnFamilyDescriptor, byte[] key, Instant timestamp,
	    Map<byte[], byte[]> columnValues) {
	File directory = columnFamilyDescriptor.getDirectory();
	File dataLog = new File(directory, "data.log");
	File commitLog = new File(directory, "commit.log");
	// TODO storeData(tab)
    }

    public void delete(TableDescriptor tableDescriptor, Delete delete) {
	// TODO Auto-generated method stub
    }

}
