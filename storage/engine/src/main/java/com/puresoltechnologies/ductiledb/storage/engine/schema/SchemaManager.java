package com.puresoltechnologies.ductiledb.storage.engine.schema;

import java.util.Iterator;

/**
 * This is the central SchemaManager.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface SchemaManager {

    public Iterator<NamespaceDescriptor> getNamespaces();

    public NamespaceDescriptor createNamespace(String namespaceName) throws SchemaException;

    public void dropNamespace(NamespaceDescriptor namespace) throws SchemaException;

    public Iterator<TableDescriptor> getTables(NamespaceDescriptor namespace);

    public TableDescriptor createTable(NamespaceDescriptor namespace, String tableName) throws SchemaException;

    public void dropTable(TableDescriptor table) throws SchemaException;

    public Iterator<ColumnFamilyDescriptor> getColumnFamilies(TableDescriptor table);

    public ColumnFamilyDescriptor createColumnFamily(TableDescriptor table, String columnFamilyName)
	    throws SchemaException;

    public void dropColumnFamily(ColumnFamilyDescriptor columnFamilyName) throws SchemaException;

}
