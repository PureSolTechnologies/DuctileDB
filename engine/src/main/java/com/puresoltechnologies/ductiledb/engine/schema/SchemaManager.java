package com.puresoltechnologies.ductiledb.engine.schema;

import com.puresoltechnologies.ductiledb.engine.cf.index.secondary.SecondaryIndexDescriptor;
import com.puresoltechnologies.ductiledb.logstore.Key;
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

    public TableDescriptor createTable(NamespaceDescriptor namespaceDescriptor, String tableName, String description)
	    throws SchemaException, StorageException;

    public TableDescriptor createTableIfNotPresent(NamespaceDescriptor namespaceDescriptor, String tableName,
	    String description) throws SchemaException, StorageException;

    public void dropTable(TableDescriptor tableDescriptor) throws SchemaException;

    public Iterable<ColumnFamilyDescriptor> getColumnFamilies(TableDescriptor tableDescriptor);

    public ColumnFamilyDescriptor getColumnFamily(TableDescriptor tableDescriptor, Key columnFamilyName);

    public ColumnFamilyDescriptor createColumnFamily(TableDescriptor tableDescriptor, Key columnFamilyName)
	    throws SchemaException, StorageException;

    public ColumnFamilyDescriptor createColumnFamilyIfNotPresent(TableDescriptor tableDescriptor, Key columnFamilyName)
	    throws SchemaException, StorageException;

    public void dropColumnFamily(ColumnFamilyDescriptor columnFamilyDescriptor) throws SchemaException;

    public Iterable<SecondaryIndexDescriptor> getIndizes(ColumnFamilyDescriptor columnFamilyDescriptor);

    public SecondaryIndexDescriptor getIndex(ColumnFamilyDescriptor columnFamilyDescriptor, String name);

    public void createIndex(ColumnFamilyDescriptor columnFamilyDescriptor, SecondaryIndexDescriptor indexDescriptor)
	    throws SchemaException;

    public void dropIndex(String name, ColumnFamilyDescriptor columnFamilyDescriptor);

}
