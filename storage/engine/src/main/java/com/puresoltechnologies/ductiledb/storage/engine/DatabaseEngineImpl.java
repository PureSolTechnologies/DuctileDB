package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.commons.misc.StopWatch;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyEngineImpl;
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
public class DatabaseEngineImpl implements DatabaseEngine {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseEngineImpl.class);

    private boolean closed = false;
    private final Storage storage;
    private final String storageName;
    private final DatabaseEngineConfiguration configuration;
    private final File storageDirectory;
    private final SchemaManager schemaManager;
    private final Map<String, NamespaceEngineImpl> namespaceEngines = new HashMap<>();

    public DatabaseEngineImpl(Storage storage, String storageName, DatabaseEngineConfiguration configuration) {
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

    private SchemaManager initializeStorage(Storage storage) {
	try {
	    storage.createDirectory(storageDirectory);
	    return new SchemaManagerImpl(this, storageDirectory);
	} catch (IOException e) {
	    throw new StorageException("Could not initialize storage engine.");
	}
    }

    private void initializeNamespaceEngines() {
	for (NamespaceDescriptor namespaceDescriptor : schemaManager.getNamespaces()) {
	    addNamespace(namespaceDescriptor);
	}
    }

    public void addNamespace(NamespaceDescriptor namespaceDescriptor) {
	namespaceEngines.put(namespaceDescriptor.getName(),
		new NamespaceEngineImpl(storage, namespaceDescriptor, configuration));
    }

    public void setRunCompactions(boolean runCompaction) {
	namespaceEngines.values().forEach(engine -> engine.setRunCompactions(runCompaction));
    }

    public void runCompaction() {
	namespaceEngines.values().forEach(engine -> engine.runCompaction());
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

    @Override
    public final boolean isClosed() {
	return closed;
    }

    public final Storage getStorage() {
	return storage;
    }

    @Override
    public final String getStoreName() {
	return storageName;
    }

    @Override
    public final SchemaManager getSchemaManager() {
	return schemaManager;
    }

    @Override
    public Table getTable(TableDescriptor tableDescriptor) {
	NamespaceDescriptor namespace = tableDescriptor.getNamespace();
	return new Table(namespaceEngines.get(namespace.getName()).getTableEngine(tableDescriptor.getName()),
		tableDescriptor);
    }

    @Override
    public Table getTable(String namespaceName, String tableName) {
	NamespaceEngineImpl namespaceEngineImpl = namespaceEngines.get(namespaceName);
	TableDescriptor tableDescriptor = schemaManager.getNamespace(namespaceName).getTable(tableName);
	return new Table(namespaceEngineImpl.getTableEngine(tableDescriptor.getName()), tableDescriptor);
    }

    public NamespaceEngineImpl getNamespaceEngine(String namespaceName) {
	return namespaceEngines.get(namespaceName);
    }

    public TableEngineImpl getTableEngine(TableDescriptor table) {
	NamespaceDescriptor namespace = table.getNamespace();
	return getNamespaceEngine(namespace.getName()).getTableEngine(table.getName());
    }

    public ColumnFamilyEngineImpl getColumnFamilyEngine(ColumnFamilyDescriptor columnFamily) {
	return getTableEngine(columnFamily.getTable()).getColumnFamilyEngine(columnFamily.getName());
    }

    public void addTable(TableDescriptor tableDescriptor) {
	namespaceEngines.get(tableDescriptor.getNamespace().getName()).addTable(tableDescriptor);
    }

    public void addColumnFamily(ColumnFamilyDescriptor columnFamilyDescriptor) {
	TableDescriptor tableDescriptor = columnFamilyDescriptor.getTable();
	NamespaceDescriptor namespaceDescriptor = tableDescriptor.getNamespace();
	NamespaceEngineImpl namespaceEngine = namespaceEngines.get(namespaceDescriptor.getName());
	TableEngineImpl tableEngine = namespaceEngine.getTableEngine(tableDescriptor.getName());
	tableEngine.addColumnFamily(columnFamilyDescriptor);
    }

    public void dropTable(TableDescriptor tableDescriptor) {
	namespaceEngines.get(tableDescriptor.getNamespace().getName()).dropTable(tableDescriptor);
    }

    public void dropColumnFamily(ColumnFamilyDescriptor columnFamilyDescriptor) {
	TableDescriptor tableDescriptor = columnFamilyDescriptor.getTable();
	NamespaceDescriptor namespaceDescriptor = tableDescriptor.getNamespace();
	NamespaceEngineImpl namespaceEngine = namespaceEngines.get(namespaceDescriptor.getName());
	TableEngineImpl tableEngine = namespaceEngine.getTableEngine(tableDescriptor.getName());
	tableEngine.dropColumnFamily(columnFamilyDescriptor);
    }

    @Override
    public String toString() {
	return "DatabaseEngine: " + storageName;
    }
}
