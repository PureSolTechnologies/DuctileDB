package com.puresoltechnologies.ductiledb.core.tables.ddl;

import java.util.HashSet;
import java.util.Set;

import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.TableStore;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.columns.ColumnType;
import com.puresoltechnologies.ductiledb.core.tables.dml.TableRowIterable;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.Key;
import com.puresoltechnologies.ductiledb.storage.engine.schema.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManager;
import com.puresoltechnologies.ductiledb.storage.engine.schema.TableDescriptor;

public class CreateTableImpl implements CreateTable {

    private final TableStoreImpl tableStore;
    private final String namespace;
    private final String name;
    private final TableDefinitionImpl tableDefinition;

    public CreateTableImpl(TableStoreImpl tableStore, String namespace, String name) {
	super();
	this.tableStore = tableStore;
	this.namespace = namespace;
	this.name = name;
	tableDefinition = new TableDefinitionImpl(namespace, name);
    }

    @Override
    public TableRowIterable execute(TableStore tableStore) throws ExecutionException {
	if ("system".equals(namespace)) {
	    throw new ExecutionException("Creating tables in 'system' namespace is not allowed.");
	}
	if (tableDefinition.getPrimaryKey().size() == 0) {
	    throw new ExecutionException("No primary key was defined.");
	}
	try {
	    DatabaseEngineImpl storageEngine = ((TableStoreImpl) tableStore).getStorageEngine();
	    SchemaManager schemaManager = storageEngine.getSchemaManager();
	    NamespaceDescriptor namespaceDescriptor = schemaManager.getNamespace(namespace);
	    TableDescriptor tableDescriptor = schemaManager.createTable(namespaceDescriptor, name);

	    Set<String> columnFamilies = new HashSet<>();
	    for (ColumnDefinition<?> columnDefinition : tableDefinition.getColumnDefinitions()) {
		columnFamilies.add(columnDefinition.getColumnFamily());
	    }
	    for (String columnFamily : columnFamilies) {
		schemaManager.createColumnFamily(tableDescriptor, Key.of(columnFamily));
	    }
	    ((TableStoreImpl) tableStore).getSchema().addTableDefinition(namespace, tableDefinition);
	    return null;
	} catch (StorageException | SchemaException e) {
	    throw new ExecutionException("Could not create table '" + namespace + "." + name + "'.");
	}
    }

    @Override
    public void addColumn(String columnFamily, String name, ColumnType type) {
	tableDefinition.addColumn(columnFamily, name, type);
    }

    @Override
    public void setPrimaryKey(String... columns) {
	tableDefinition.setPrimaryKey(columns);
    }

}
