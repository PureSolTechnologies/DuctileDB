package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.commons.misc.StopWatch;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManager;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManagerImpl;
import com.puresoltechnologies.ductiledb.storage.engine.schema.TableDescriptor;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This class is the database engine class. It supports a schema, and multiple
 * big table storages organized in column families. It is using the
 * {@link TableEngine} to store the separate tables.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DatabaseEngine implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseEngine.class);

    private boolean closed = false;
    private final Storage storage;
    private final String storageName;
    private final DatabaseEngineConfiguration configuration;
    private final File storageDirectory;
    private final SchemaManager schemaManager;
    private final Map<String, NamespaceEngine> namespaceEngines = new HashMap<>();

    public DatabaseEngine(Storage storage, String storageName, DatabaseEngineConfiguration configuration)
	    throws StorageException {
	this.storage = storage;
	this.storageName = storageName;
	this.configuration = configuration;
	this.storageDirectory = new File(storageName);
	logger.info("Starting database engine '" + storageName + "'...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	this.schemaManager = initializeStorage(storage);
	initializeNamespaceEngines();
	stopWatch.stop();
	logger.info("Database engine '" + storageName + "' started in " + stopWatch.getMillis() + "ms.");
    }

    private SchemaManager initializeStorage(Storage storage) throws StorageException {
	try {
	    storage.createDirectory(storageDirectory);
	    return new SchemaManagerImpl(this, storageDirectory);
	} catch (IOException e) {
	    throw new StorageException("Could not initialize storage engine.");
	}
    }

    private void initializeNamespaceEngines() throws StorageException {
	for (NamespaceDescriptor namespaceDescriptor : schemaManager.getNamespaces()) {
	    namespaceEngines.put(namespaceDescriptor.getName(),
		    new NamespaceEngine(storage, namespaceDescriptor, configuration));
	}
    }

    @Override
    public void close() throws IOException {
	logger.info("Closing database engine '" + storageName + "'...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	for (NamespaceEngine namespaceEngine : namespaceEngines.values()) {
	    namespaceEngine.close();
	}
	closed = true;
	storage.close();
	stopWatch.stop();
	logger.info("Database engine '" + storageName + "' closed in " + stopWatch.getMillis() + "ms.");
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

    public void put(TableDescriptor tableDescriptor, Put put) {
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
	// TODO storeData(tab)
    }

    public void delete(TableDescriptor tableDescriptor, Delete delete) {
	// TODO Auto-generated method stub
    }

}
