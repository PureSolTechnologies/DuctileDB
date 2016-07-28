package com.puresoltechnologies.ductiledb.storage.engine.schema;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;

/**
 * This is the central SchemaManager.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface SchemaManager {

    public Iterable<NamespaceDescriptor> getNamespaces();

    public NamespaceDescriptor getNamespace(String namespaceName);

    public NamespaceDescriptor createNamespace(String namespaceName) throws SchemaException, StorageException;

    public NamespaceDescriptor createNamespaceIfNotPresent(String namespaceName)
	    throws SchemaException, StorageException;

    public void dropNamespace(NamespaceDescriptor namespaceDescriptor) throws SchemaException;

    public Iterable<TableDescriptor> getTables(NamespaceDescriptor namespaceDescriptor);

    public TableDescriptor getTable(NamespaceDescriptor namespaceDescriptor, String tableName);

    public TableDescriptor getTable(String tableName);

    public TableDescriptor createTable(NamespaceDescriptor namespaceDescriptor, String tableName)
	    throws SchemaException, StorageException;

    public TableDescriptor createTableIfNotPresent(NamespaceDescriptor namespaceDescriptor, String tableName)
	    throws SchemaException, StorageException;

    public void dropTable(TableDescriptor tableDescriptor) throws SchemaException;

    public Iterable<ColumnFamilyDescriptor> getColumnFamilies(TableDescriptor tableDescriptor);

    public ColumnFamilyDescriptor getColumnFamily(TableDescriptor tableDescriptor, byte[] columnFamilyName);

    public ColumnFamilyDescriptor createColumnFamily(TableDescriptor tableDescriptor, byte[] columnFamilyName)
	    throws SchemaException, StorageException;

    public ColumnFamilyDescriptor createColumnFamilyIfNotPresent(TableDescriptor tableDescriptor,
	    byte[] columnFamilyName) throws SchemaException, StorageException;

    public void dropColumnFamily(ColumnFamilyDescriptor columnFamilyDescriptor) throws SchemaException;

}
