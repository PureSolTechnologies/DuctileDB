package com.puresoltechnologies.ductiledb.storage.engine.schema;

import static com.puresoltechnologies.ductiledb.storage.engine.EngineChecks.checkIdentifier;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.EngineChecks;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class SchemaManagerImpl implements SchemaManager {

    private static final Logger logger = LoggerFactory.getLogger(SchemaManagerImpl.class);

    private final Map<String, NamespaceDescriptor> namespaces = new HashMap<>();

    private final DatabaseEngineImpl databaseEngine;
    private final Storage storage;
    private final File storageDirectory;

    public SchemaManagerImpl(DatabaseEngineImpl databaseEngine, File storageDirectory) {
	this.databaseEngine = databaseEngine;
	this.storage = databaseEngine.getStorage();
	this.storageDirectory = storageDirectory;
	readSchema();
    }

    private void readSchema() {
	for (NamespaceDescriptor namespace : readNamespaces()) {
	    namespaces.put(namespace.getName(), namespace);
	    for (TableDescriptor table : readTables(namespace)) {
		namespace.addTable(table);
		for (ColumnFamilyDescriptor columnFamily : readColumnFamilies(table)) {
		    table.addColumnFamily(columnFamily);
		}
	    }
	}
    }

    private NamespaceIterable readNamespaces() {
	return new NamespaceIterable(storage.list(storageDirectory));
    }

    private class NamespaceIterable implements Iterable<NamespaceDescriptor> {

	private final Iterator<File> iterator;

	public NamespaceIterable(Iterable<File> iterable) {
	    this.iterator = iterable.iterator();
	}

	@Override
	public Iterator<NamespaceDescriptor> iterator() {
	    return new Iterator<NamespaceDescriptor>() {
		@Override
		public boolean hasNext() {
		    return iterator.hasNext();
		}

		@Override
		public NamespaceDescriptor next() {
		    File directory = iterator.next();
		    return new NamespaceDescriptor(directory.getName(), storage, directory);
		}
	    };
	}

    }

    private TableIterable readTables(NamespaceDescriptor namespaceDescriptor) {
	return new TableIterable(storage.list(namespaceDescriptor.getDirectory()), namespaceDescriptor);
    }

    private class TableIterable implements Iterable<TableDescriptor> {

	private final Iterator<File> iterator;
	private final NamespaceDescriptor namespace;

	public TableIterable(Iterable<File> iterable, NamespaceDescriptor namespace) {
	    this.iterator = iterable.iterator();
	    this.namespace = namespace;
	}

	@Override
	public Iterator<TableDescriptor> iterator() {
	    return new Iterator<TableDescriptor>() {
		@Override
		public boolean hasNext() {
		    return iterator.hasNext();
		}

		@Override
		public TableDescriptor next() {
		    File directory = iterator.next();
		    return new TableDescriptor(directory.getName(), namespace, directory);
		}
	    };
	}

    }

    private ColumnFamilyIterable readColumnFamilies(TableDescriptor tableDescriptor) {
	return new ColumnFamilyIterable(storage.list(tableDescriptor.getDirectory()), tableDescriptor);
    }

    private class ColumnFamilyIterable implements Iterable<ColumnFamilyDescriptor> {

	private final Iterator<File> iterator;
	private final TableDescriptor table;

	public ColumnFamilyIterable(Iterable<File> iterable, TableDescriptor table) {
	    this.iterator = iterable.iterator();
	    this.table = table;
	}

	@Override
	public Iterator<ColumnFamilyDescriptor> iterator() {
	    return new Iterator<ColumnFamilyDescriptor>() {
		@Override
		public boolean hasNext() {
		    return iterator.hasNext();
		}

		@Override
		public ColumnFamilyDescriptor next() {
		    File directory = iterator.next();
		    return new ColumnFamilyDescriptor(Bytes.toBytes(directory.getName()), table, directory);
		}
	    };
	}

    }

    private String getStoreName() {
	return databaseEngine.getStoreName();
    }

    @Override
    public Iterable<NamespaceDescriptor> getNamespaces() {
	return namespaces.values();
    }

    @Override
    public NamespaceDescriptor getNamespace(String namespaceName) {
	return namespaces.get(namespaceName);
    }

    @Override
    public NamespaceDescriptor createNamespace(String namespaceName) throws SchemaException, StorageException {
	if (!checkIdentifier(namespaceName)) {
	    throw new SchemaException("Namespace name '" + namespaceName
		    + "' is invalid. Identifiers have to match pattern '" + EngineChecks.IDENTIFIED_FORM + "'.");
	}
	logger.info("Creating namespace '" + namespaceName + "' in storage '" + getStoreName() + "'...");
	if (namespaces.containsKey(namespaceName)) {
	    throw new SchemaException("Namespace '" + namespaceName + "' is already present.");
	}
	try {
	    File namespaceDirectory = new File(storageDirectory, namespaceName);
	    storage.createDirectory(namespaceDirectory);
	    try (BufferedOutputStream metadataFile = storage
		    .create(new File(namespaceDirectory, "metadata.properties"))) {
		Properties properties = new Properties();
		properties.put("namespace.creation.time", new Date().toString());
		properties.put("namespace.name", namespaceName);
		properties.put("namespace.storage.directory", storageDirectory.toString());
		properties.put("namespace.storage.name", getStoreName());
		properties.store(metadataFile, "Meta data for namespace.");

		NamespaceDescriptor namespaceDescriptor = new NamespaceDescriptor(namespaceName, storage,
			namespaceDirectory);
		databaseEngine.addNamespace(namespaceDescriptor);
		namespaces.put(namespaceDescriptor.getName(), namespaceDescriptor);
		return namespaceDescriptor;
	    }
	} catch (IOException e) {
	    throw new SchemaException("Could not create schema '" + namespaceName + "'.", e);
	}
    }

    @Override
    public NamespaceDescriptor createNamespaceIfNotPresent(String namespaceName)
	    throws SchemaException, StorageException {
	NamespaceDescriptor namespaceDescriptor = getNamespace(namespaceName);
	if (namespaceDescriptor == null) {
	    return createNamespace(namespaceName);
	} else {
	    return namespaceDescriptor;
	}
    }

    @Override
    public void dropNamespace(NamespaceDescriptor namespaceDescriptor) throws SchemaException {
	try {
	    logger.info("Dropping '" + namespaceDescriptor + "' in storage '" + getStoreName() + "'...");
	    storage.removeDirectory(namespaceDescriptor.getDirectory(), true);
	    namespaces.remove(namespaceDescriptor.getName());
	} catch (IOException e) {
	    throw new SchemaException("Could not drop schema '" + namespaceDescriptor + "'.", e);
	}
    }

    @Override
    public Iterable<TableDescriptor> getTables(NamespaceDescriptor namespaceDescriptor) {
	return namespaceDescriptor.getTables();
    }

    @Override
    public TableDescriptor getTable(NamespaceDescriptor namespaceDescriptor, String tableName) {
	return namespaceDescriptor.getTable(tableName);
    }

    @Override
    public TableDescriptor createTable(NamespaceDescriptor namespaceDescriptor, String tableName)
	    throws SchemaException, StorageException {
	if (!checkIdentifier(tableName)) {
	    throw new SchemaException("Table name '" + tableName + "' is invalid. Identifiers have to match pattern '"
		    + EngineChecks.IDENTIFIED_FORM + "'.");
	}
	logger.info("Creating table '" + namespaceDescriptor.getName() + "." + tableName + "' in storage '"
		+ getStoreName() + "'...");
	NamespaceDescriptor presentDescriptor = namespaces.get(namespaceDescriptor.getName());
	if (!namespaceDescriptor.equals(presentDescriptor)) {
	    throw new StorageException("Namespace '" + namespaceDescriptor.getName() + "' is not present.");
	}
	if (presentDescriptor.getTable(tableName) != null) {
	    throw new StorageException(
		    "Table '" + namespaceDescriptor.getName() + "." + tableName + "' is already present.");
	}
	try {
	    File tableDirectory = new File(namespaceDescriptor.getDirectory(), tableName);
	    storage.createDirectory(tableDirectory);
	    try (BufferedOutputStream metadataFile = storage.create(new File(tableDirectory, "metadata.properties"))) {
		Properties properties = new Properties();
		properties.put("table.creation.time", new Date().toString());
		properties.put("table.name", tableName);
		properties.put("table.namespace.name", namespaceDescriptor.getName());
		properties.put("table.storage.directory", storageDirectory.toString());
		properties.put("table.storage.name", getStoreName());
		properties.store(metadataFile, "Meta data for table.");

		TableDescriptor tableDescriptor = new TableDescriptor(tableName, namespaceDescriptor, tableDirectory);
		namespaceDescriptor.addTable(tableDescriptor);
		databaseEngine.addTable(tableDescriptor);
		return tableDescriptor;
	    }
	} catch (IOException e) {
	    throw new SchemaException("Could not create table '" + namespaceDescriptor + "." + tableName + "'.", e);
	}
    }

    @Override
    public TableDescriptor createTableIfNotPresent(NamespaceDescriptor namespaceDescriptor, String tableName)
	    throws SchemaException, StorageException {
	TableDescriptor tableDescriptor = getTable(namespaceDescriptor, tableName);
	if (tableDescriptor == null) {
	    return createTable(namespaceDescriptor, tableName);
	} else {
	    return tableDescriptor;
	}
    }

    @Override
    public void dropTable(TableDescriptor tableDescriptor) throws SchemaException {
	try {
	    logger.info("Dropping '" + tableDescriptor + "' in storage '" + getStoreName() + "'...");
	    storage.removeDirectory(tableDescriptor.getDirectory(), true);
	    tableDescriptor.getNamespace().removeTable(tableDescriptor);
	} catch (IOException e) {
	    throw new SchemaException("Could not drop schema '" + tableDescriptor + "'.", e);
	}
    }

    @Override
    public Iterable<ColumnFamilyDescriptor> getColumnFamilies(TableDescriptor tableDescriptor) {
	return tableDescriptor.getColumnFamilies();
    }

    @Override
    public ColumnFamilyDescriptor getColumnFamily(TableDescriptor tableDescriptor, byte[] columnFamilyName) {
	return tableDescriptor.getColumnFamily(columnFamilyName);
    }

    @Override
    public ColumnFamilyDescriptor createColumnFamily(TableDescriptor tableDescriptor, byte[] columnFamilyName)
	    throws SchemaException, StorageException {
	if ((columnFamilyName == null) || (columnFamilyName.length == 0)) {
	    throw new SchemaException("Column family name '" + Bytes.toHumanReadableString(columnFamilyName)
		    + "' is invalid. Identifiers have to match pattern '" + EngineChecks.IDENTIFIED_FORM + "'.");
	}
	NamespaceDescriptor namespaceDescriptor = tableDescriptor.getNamespace();
	File columnFamilyDirectory = new File(tableDescriptor.getDirectory(), Bytes.toHexString(columnFamilyName));
	ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(columnFamilyName, tableDescriptor,
		columnFamilyDirectory);
	logger.info("Creating column family '" + columnFamilyDescriptor + "' in storage '" + getStoreName() + "'...");
	NamespaceDescriptor presentDescriptor = namespaces.get(namespaceDescriptor.getName());
	if (!namespaceDescriptor.equals(presentDescriptor)) {
	    throw new StorageException("Namespace '" + namespaceDescriptor + "' is not present.");
	}
	TableDescriptor presentTable = presentDescriptor.getTable(tableDescriptor.getName());
	if (presentTable == null) {
	    throw new StorageException("Table '" + tableDescriptor + "' is not present.");
	}
	if (!presentTable.equals(tableDescriptor)) {
	    throw new StorageException("Table '" + tableDescriptor + "' is not present.");
	}
	ColumnFamilyDescriptor columnFamily = presentTable.getColumnFamily(columnFamilyName);
	if (columnFamily != null) {
	    throw new StorageException("Column family '" + columnFamilyDescriptor + "' in storage '" + getStoreName()
		    + "' is already present.");
	}
	try {
	    storage.createDirectory(columnFamilyDirectory);
	    storage.createDirectory(columnFamilyDescriptor.getIndexDirectory());
	    try (BufferedOutputStream metadataFile = storage
		    .create(new File(columnFamilyDirectory, "metadata.properties"))) {
		Properties properties = new Properties();
		properties.put("cf.creation.time", new Date().toString());
		properties.put("cf.name", Bytes.toHexString(columnFamilyName));
		properties.put("cf.name.ascii", Bytes.toString(columnFamilyName));
		properties.put("cf.namespace.name", namespaceDescriptor.getName());
		properties.put("cf.table.name", tableDescriptor.getName());
		properties.put("cf.storage.directory", storageDirectory.toString());
		properties.put("cf.storage.name", getStoreName());
		properties.store(metadataFile, "Meta data for column family.");

		tableDescriptor.addColumnFamily(columnFamilyDescriptor);
		databaseEngine.addColumnFamily(columnFamilyDescriptor);
		return columnFamilyDescriptor;
	    }
	} catch (IOException e) {
	    throw new SchemaException("Could not create column family '" + columnFamilyDescriptor + "'.", e);
	}
    }

    @Override
    public ColumnFamilyDescriptor createColumnFamilyIfNotPresent(TableDescriptor tableDescriptor,
	    byte[] columnFamilyName) throws SchemaException, StorageException {
	ColumnFamilyDescriptor columnFamilyDescriptor = getColumnFamily(tableDescriptor, columnFamilyName);
	if (columnFamilyDescriptor == null) {
	    return createColumnFamily(tableDescriptor, columnFamilyName);
	} else {
	    return columnFamilyDescriptor;
	}
    }

    @Override
    public void dropColumnFamily(ColumnFamilyDescriptor columnFamilyDescriptor) throws SchemaException {
	try {
	    logger.info("Dropping '" + columnFamilyDescriptor + "' in storage '" + getStoreName() + "'...");
	    storage.removeDirectory(columnFamilyDescriptor.getDirectory(), true);
	    columnFamilyDescriptor.getTable().removeColumnFamily(columnFamilyDescriptor);
	} catch (IOException e) {
	    throw new SchemaException("Could not drop column family '" + columnFamilyDescriptor + "'.", e);
	}
    }

    @Override
    public Iterable<SecondaryIndexDescriptor> getIndizes(ColumnFamilyDescriptor columnFamilyDescriptor) {
	ColumnFamilyEngineImpl columnFamilyEngine = databaseEngine.getColumnFamilyEngine(columnFamilyDescriptor);
	return columnFamilyEngine.getIndizes();
    }

    @Override
    public SecondaryIndexDescriptor getIndex(String name, ColumnFamilyDescriptor columnFamilyDescriptor) {
	ColumnFamilyEngineImpl columnFamilyEngine = databaseEngine.getColumnFamilyEngine(columnFamilyDescriptor);
	return columnFamilyEngine.getIndex(name);
    }

    @Override
    public void createIndex(ColumnFamilyDescriptor columnFamilyDescriptor, SecondaryIndexDescriptor indexDescriptor)
	    throws SchemaException {
	ColumnFamilyEngineImpl columnFamilyEngine = databaseEngine.getColumnFamilyEngine(columnFamilyDescriptor);
	columnFamilyEngine.createIndex(indexDescriptor);
    }

    @Override
    public void dropIndex(String name, ColumnFamilyDescriptor columnFamilyDescriptor) {
	ColumnFamilyEngineImpl columnFamilyEngine = databaseEngine.getColumnFamilyEngine(columnFamilyDescriptor);
	columnFamilyEngine.dropIndex(name);
    }

}
