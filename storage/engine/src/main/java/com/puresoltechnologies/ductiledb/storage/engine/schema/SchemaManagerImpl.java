package com.puresoltechnologies.ductiledb.storage.engine.schema;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class SchemaManagerImpl implements SchemaManager {

    private final Storage storage;
    private final File storageDirectory;

    public SchemaManagerImpl(Storage storage, File storageDirectory) {
	this.storage = storage;
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
    public NamespaceDescriptor createNamespace(String namespaceName) throws SchemaException {
	try {
	    File namespaceDirectory = new File(storageDirectory, namespaceName);
	    storage.createDirectory(namespaceDirectory);
	    return new NamespaceDescriptor(namespaceName, storage, namespaceDirectory);
	} catch (IOException e) {
	    throw new SchemaException("Could not create schema '" + namespaceName + "'.", e);
	}
    }

    @Override
    public void dropNamespace(NamespaceDescriptor namespace) throws SchemaException {
	try {
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
    public TableDescriptor createTable(NamespaceDescriptor namespace, String tableName) throws SchemaException {
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
    public ColumnFamilyDescriptor createColumnFamily(TableDescriptor table, String columnFamilyName)
	    throws SchemaException {
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
	    storage.removeDirectory(columnFamily.getDirectory(), true);
	} catch (IOException e) {
	    throw new SchemaException("Could not drop column family '" + columnFamily + "'.", e);
	}
    }

}
