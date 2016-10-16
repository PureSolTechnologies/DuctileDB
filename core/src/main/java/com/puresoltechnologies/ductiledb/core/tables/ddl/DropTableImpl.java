package com.puresoltechnologies.ductiledb.core.tables.ddl;

import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.dml.TableRowIterable;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.schema.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManager;
import com.puresoltechnologies.ductiledb.storage.engine.schema.TableDescriptor;

public class DropTableImpl implements DropTable {

    private final TableStoreImpl tableStore;
    private final String namespace;
    private final String name;

    public DropTableImpl(TableStoreImpl tableStore, String namespace, String name) {
	super();
	this.tableStore = tableStore;
	this.namespace = namespace;
	this.name = name;
    }

    @Override
    public TableRowIterable execute() throws ExecutionException {
	if ("system".equals(namespace)) {
	    throw new ExecutionException("Dropping tables from 'system' namespace is not allowed.");
	}
	try {
	    DatabaseEngineImpl storageEngine = tableStore.getStorageEngine();
	    SchemaManager schemaManager = storageEngine.getSchemaManager();
	    NamespaceDescriptor namespaceDescriptor = schemaManager.getNamespace(namespace);
	    TableDescriptor tableDescriptor = schemaManager.getTable(namespaceDescriptor, name);
	    schemaManager.dropTable(tableDescriptor);
	    tableStore.getSchema().removeTableDefinition(namespace, tableDescriptor.getName());
	    return null;
	} catch (SchemaException e) {
	    throw new ExecutionException("Could not drop table '" + namespace + "." + name + "'.", e);
	}
    }

}
