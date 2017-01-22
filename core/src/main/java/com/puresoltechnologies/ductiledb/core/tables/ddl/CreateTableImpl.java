package com.puresoltechnologies.ductiledb.core.tables.ddl;

import java.util.HashSet;
import java.util.Set;

import com.puresoltechnologies.ductiledb.bigtable.TableDescriptor;
import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.columns.ColumnType;
import com.puresoltechnologies.ductiledb.core.tables.dml.TableRowIterable;
import com.puresoltechnologies.ductiledb.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.engine.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.engine.schema.SchemaManager;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;

public class CreateTableImpl extends AbstractDDLStatement implements CreateTable {

    private final String namespace;
    private final String name;
    private final TableDefinitionImpl tableDefinition;

    public CreateTableImpl(TableStoreImpl tableStore, String namespace, String name, String description) {
	super(tableStore);
	this.namespace = namespace;
	this.name = name;
	tableDefinition = new TableDefinitionImpl(namespace, name, description);
    }

    @Override
    public void addColumn(String columnFamily, String name, ColumnType type, String description) {
	tableDefinition.addColumn(columnFamily, name, type, description);
    }

    @Override
    public void setPrimaryKey(String... columns) {
	tableDefinition.setPrimaryKey(columns);
    }

    @Override
    public TableRowIterable execute() throws ExecutionException {
	if ("system".equals(namespace)) {
	    throw new ExecutionException("Creating tables in 'system' namespace is not allowed.");
	}
	if (tableDefinition.getPrimaryKey().size() == 0) {
	    throw new ExecutionException("No primary key was defined.");
	}
	try {
	    TableStoreImpl tableStore = getTableStore();
	    DatabaseEngineImpl storageEngine = tableStore.getStorageEngine();
	    SchemaManager schemaManager = storageEngine.getSchemaManager();
	    NamespaceDescriptor namespaceDescriptor = schemaManager.getNamespace(namespace);
	    TableDescriptor tableDescriptor = schemaManager.createTable(namespaceDescriptor, name,
		    tableDefinition.getDescription());

	    Set<String> columnFamilies = new HashSet<>();
	    for (ColumnDefinition<?> columnDefinition : tableDefinition.getColumnDefinitions()) {
		columnFamilies.add(columnDefinition.getColumnFamily());
	    }
	    for (String columnFamily : columnFamilies) {
		schemaManager.createColumnFamily(tableDescriptor, Key.of(columnFamily));
	    }
	    tableStore.getSchema().addTableDefinition(namespace, tableDefinition);
	    return null;
	} catch (StorageException | SchemaException e) {
	    throw new ExecutionException("Could not create table '" + namespace + "." + name + "'.");
	}
    }

}
