package com.puresoltechnologies.ductiledb.storage.engine.schema;

import static com.puresoltechnologies.ductiledb.storage.engine.utils.EngineChecks.checkIdentifier;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.storage.engine.StorageEngine;
import com.puresoltechnologies.ductiledb.storage.engine.utils.EngineChecks;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class SchemaManagerImpl implements SchemaManager {

    private static final Logger logger = LoggerFactory.getLogger(SchemaManagerImpl.class);

    private final StorageEngine storageEngine;
    private final Storage storage;
    private final File storageDirectory;

    public SchemaManagerImpl(StorageEngine storageEngine, File storageDirectory) {
	this.storageEngine = storageEngine;
	this.storage = storageEngine.getStorage();
	this.storageDirectory = storageDirectory;
    }

    @Override
    public Iterator<NamespaceDescriptor> getNamespaces() {
	return new NamespaceIterator(storage.list(storageDirectory));
    }

    private class NamespaceIterator implements Iterator<NamespaceDescriptor> {

	private final Iterator<File> iterator;

	public NamespaceIterator(Iterator<File> iterator) {
	    this.iterator = iterator;
	}

	@Override
	public boolean hasNext() {
	    return iterator.hasNext();
	}

	@Override
	public NamespaceDescriptor next() {
	    File directory = iterator.next();
	    return new NamespaceDescriptor(directory.getName(), storage, directory);
	}

    }

    @Override
    public NamespaceDescriptor getNamespace(String namespaceName) {
	File directory = new File(storageDirectory, namespaceName);
	if ((!storage.exists(directory)) || (!storage.isDirectory(directory))) {
	    return null;
	}
	return new NamespaceDescriptor(namespaceName, storage, directory);
    }

    @Override
    public NamespaceDescriptor createNamespace(String namespaceName) throws SchemaException {
	if (!checkIdentifier(namespaceName)) {
	    throw new SchemaException("Namespace name '" + namespaceName
		    + "' is invalid. Identifiers have to match pattern '" + EngineChecks.IDENTIFIED_FORM + "'.");
	}
	logger.info("Creating namespace '" + namespaceName + "' in storage '" + getStoreName() + "'...");
	try {
	    File namespaceDirectory = new File(storageDirectory, namespaceName);
	    storage.createDirectory(namespaceDirectory);
	    return new NamespaceDescriptor(namespaceName, storage, namespaceDirectory);
	} catch (IOException e) {
	    throw new SchemaException("Could not create schema '" + namespaceName + "'.", e);
	}
    }

    private String getStoreName() {
	return storageEngine.getStoreName();
    }

    @Override
    public void dropNamespace(NamespaceDescriptor namespace) throws SchemaException {
	try {
	    logger.info("Dropping '" + namespace + "' in storage '" + getStoreName() + "'...");
	    storage.removeDirectory(namespace.getDirectory(), true);
	} catch (IOException e) {
	    throw new SchemaException("Could not drop schema '" + namespace + "'.", e);
	}
    }

    @Override
    public Iterator<TableDescriptor> getTables(NamespaceDescriptor namespace) {
	return new TableIterator(storage.list(namespace.getDirectory()), namespace);
    }

    private class TableIterator implements Iterator<TableDescriptor> {

	private final Iterator<File> iterator;
	private final NamespaceDescriptor namespace;

	public TableIterator(Iterator<File> iterator, NamespaceDescriptor namespace) {
	    this.iterator = iterator;
	    this.namespace = namespace;
	}

	@Override
	public boolean hasNext() {
	    return iterator.hasNext();
	}

	@Override
	public TableDescriptor next() {
	    File directory = iterator.next();
	    return new TableDescriptor(directory.getName(), namespace, directory);
	}

    }

    @Override
    public TableDescriptor getTable(NamespaceDescriptor namespace, String tableName) {
	File directory = new File(namespace.getDirectory(), tableName);
	if ((!storage.exists(directory)) || (!storage.isDirectory(directory))) {
	    return null;
	}
	return new TableDescriptor(tableName, namespace, directory);
    }

    @Override
    public TableDescriptor getTable(String tableName) {
	String[] nameSplit = tableName.split("\\.");
	if (nameSplit.length < 2) {
	    throw new IllegalArgumentException(
		    "Table name '" + tableName + "' does not contain namespace name separated with a dot.");
	}
	if (nameSplit.length > 2) {
	    throw new IllegalArgumentException("Table name '" + tableName
		    + "' contains multiple dots, but only one is allowed to separate the namespace.");
	}
	NamespaceDescriptor namespace = getNamespace(nameSplit[0]);
	return getTable(namespace, nameSplit[1]);
    }

    @Override
    public TableDescriptor createTable(NamespaceDescriptor namespace, String tableName) throws SchemaException {
	if (!checkIdentifier(tableName)) {
	    throw new SchemaException("Table name '" + tableName + "' is invalid. Identifiers have to match pattern '"
		    + EngineChecks.IDENTIFIED_FORM + "'.");
	}
	logger.info("Creating table '" + namespace.getName() + "." + tableName + "' in storage '" + getStoreName()
		+ "'...");
	try {
	    File tableDirectory = new File(namespace.getDirectory(), tableName);
	    storage.createDirectory(tableDirectory);
	    return new TableDescriptor(tableName, namespace, tableDirectory);
	} catch (IOException e) {
	    throw new SchemaException("Could not create table '" + namespace + "." + tableName + "'.", e);
	}
    }

    @Override
    public void dropTable(TableDescriptor table) throws SchemaException {
	try {
	    logger.info("Dropping '" + table + "' in storage '" + getStoreName() + "'...");
	    storage.removeDirectory(table.getDirectory(), true);
	} catch (IOException e) {
	    throw new SchemaException("Could not drop schema '" + table + "'.", e);
	}
    }

    @Override
    public Iterator<ColumnFamilyDescriptor> getColumnFamilies(TableDescriptor table) {
	return new ColumnFamilyIterator(storage.list(table.getDirectory()), table);
    }

    private class ColumnFamilyIterator implements Iterator<ColumnFamilyDescriptor> {

	private final Iterator<File> iterator;
	private final TableDescriptor table;

	public ColumnFamilyIterator(Iterator<File> iterator, TableDescriptor table) {
	    this.iterator = iterator;
	    this.table = table;
	}

	@Override
	public boolean hasNext() {
	    return iterator.hasNext();
	}

	@Override
	public ColumnFamilyDescriptor next() {
	    File directory = iterator.next();
	    return new ColumnFamilyDescriptor(directory.getName(), table, directory);
	}

    }

    @Override
    public ColumnFamilyDescriptor getColumnFamily(TableDescriptor table, String columnFamilyName) {
	File directory = new File(table.getDirectory(), columnFamilyName);
	if ((!storage.exists(directory)) || (!storage.isDirectory(directory))) {
	    return null;
	}
	return new ColumnFamilyDescriptor(columnFamilyName, table, directory);
    }

    @Override
    public ColumnFamilyDescriptor createColumnFamily(TableDescriptor table, String columnFamilyName)
	    throws SchemaException {
	if (!checkIdentifier(columnFamilyName)) {
	    throw new SchemaException("Column family name '" + columnFamilyName
		    + "' is invalid. Identifiers have to match pattern '" + EngineChecks.IDENTIFIED_FORM + "'.");
	}
	logger.info("Creating column family '" + table.getNamespace().getName() + "." + table.getName() + "/"
		+ columnFamilyName + "' in storage '" + getStoreName() + "'...");
	try {
	    File columnFamilyDirectory = new File(table.getDirectory(), columnFamilyName);
	    storage.createDirectory(columnFamilyDirectory);
	    return new ColumnFamilyDescriptor(columnFamilyName, table, columnFamilyDirectory);
	} catch (IOException e) {
	    throw new SchemaException("Could not create column family '" + table + "." + columnFamilyName + "'.", e);
	}
    }

    @Override
    public void dropColumnFamily(ColumnFamilyDescriptor columnFamily) throws SchemaException {
	try {
	    logger.info("Dropping '" + columnFamily + "' in storage '" + getStoreName() + "'...");
	    storage.removeDirectory(columnFamily.getDirectory(), true);
	} catch (IOException e) {
	    throw new SchemaException("Could not drop column family '" + columnFamily + "'.", e);
	}
    }

}
