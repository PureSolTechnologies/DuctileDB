package com.puresoltechnologies.ductiledb.storage.engine.schema;

import static com.puresoltechnologies.ductiledb.storage.engine.EngineChecks.checkIdentifier;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.EngineChecks;
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
		    return new ColumnFamilyDescriptor(directory.getName(), table, directory);
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
	try {
	    File namespaceDirectory = new File(storageDirectory, namespaceName);
	    storage.createDirectory(namespaceDirectory);
	    NamespaceDescriptor namespaceDescriptor = new NamespaceDescriptor(namespaceName, storage,
		    namespaceDirectory);
	    databaseEngine.addNamespace(namespaceDescriptor);
	    return namespaceDescriptor;
	} catch (IOException e) {
	    throw new SchemaException("Could not create schema '" + namespaceName + "'.", e);
	}
    }

    @Override
    public void dropNamespace(NamespaceDescriptor namespaceDescriptor) throws SchemaException {
	try {
	    logger.info("Dropping '" + namespaceDescriptor + "' in storage '" + getStoreName() + "'...");
	    storage.removeDirectory(namespaceDescriptor.getDirectory(), true);
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
    public TableDescriptor createTable(NamespaceDescriptor namespaceDescriptor, String tableName)
	    throws SchemaException, StorageException {
	if (!checkIdentifier(tableName)) {
	    throw new SchemaException("Table name '" + tableName + "' is invalid. Identifiers have to match pattern '"
		    + EngineChecks.IDENTIFIED_FORM + "'.");
	}
	logger.info("Creating table '" + namespaceDescriptor.getName() + "." + tableName + "' in storage '"
		+ getStoreName() + "'...");
	try {
	    File tableDirectory = new File(namespaceDescriptor.getDirectory(), tableName);
	    storage.createDirectory(tableDirectory);
	    TableDescriptor tableDescriptor = new TableDescriptor(tableName, namespaceDescriptor, tableDirectory);
	    namespaceDescriptor.addTable(tableDescriptor);
	    databaseEngine.addTable(tableDescriptor);
	    return tableDescriptor;
	} catch (IOException e) {
	    throw new SchemaException("Could not create table '" + namespaceDescriptor + "." + tableName + "'.", e);
	}
    }

    @Override
    public void dropTable(TableDescriptor tableDescriptor) throws SchemaException {
	try {
	    logger.info("Dropping '" + tableDescriptor + "' in storage '" + getStoreName() + "'...");
	    storage.removeDirectory(tableDescriptor.getDirectory(), true);
	} catch (IOException e) {
	    throw new SchemaException("Could not drop schema '" + tableDescriptor + "'.", e);
	}
    }

    @Override
    public Iterable<ColumnFamilyDescriptor> getColumnFamilies(TableDescriptor tableDescriptor) {
	return tableDescriptor.getColumnFamilies();
    }

    @Override
    public ColumnFamilyDescriptor getColumnFamily(TableDescriptor tableDescriptor, String columnFamilyName) {
	return tableDescriptor.getColumnFamily(columnFamilyName);
    }

    @Override
    public ColumnFamilyDescriptor createColumnFamily(TableDescriptor tableDescriptor, String columnFamilyName)
	    throws SchemaException, StorageException {
	if (!checkIdentifier(columnFamilyName)) {
	    throw new SchemaException("Column family name '" + columnFamilyName
		    + "' is invalid. Identifiers have to match pattern '" + EngineChecks.IDENTIFIED_FORM + "'.");
	}
	logger.info("Creating column family '" + tableDescriptor.getNamespace().getName() + "."
		+ tableDescriptor.getName() + "/" + columnFamilyName + "' in storage '" + getStoreName() + "'...");
	try {
	    File columnFamilyDirectory = new File(tableDescriptor.getDirectory(), columnFamilyName);
	    storage.createDirectory(columnFamilyDirectory);
	    ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(columnFamilyName,
		    tableDescriptor, columnFamilyDirectory);
	    tableDescriptor.addColumnFamily(columnFamilyDescriptor);
	    databaseEngine.addColumnFamily(columnFamilyDescriptor);
	    return columnFamilyDescriptor;
	} catch (IOException e) {
	    throw new SchemaException(
		    "Could not create column family '" + tableDescriptor + "." + columnFamilyName + "'.", e);
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

}
