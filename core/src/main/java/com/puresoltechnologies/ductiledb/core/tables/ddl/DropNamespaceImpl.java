package com.puresoltechnologies.ductiledb.core.tables.ddl;

import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.dml.TableRowIterable;
import com.puresoltechnologies.ductiledb.core.tables.schema.TableStoreSchema;
import com.puresoltechnologies.ductiledb.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.engine.schema.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.engine.schema.SchemaManager;

public class DropNamespaceImpl extends AbstractDDLStatement implements DropNamespace {

    private final String name;

    public DropNamespaceImpl(TableStoreImpl tableStore, String name) {
	super(tableStore);
	this.name = name;
    }

    @Override
    public TableRowIterable execute() throws ExecutionException {
	if ("system".equals(name)) {
	    throw new ExecutionException("Dropping of 'system' namespace is not allowed.");
	}
	try {
	    TableStoreImpl tableStore = getTableStore();
	    DatabaseEngineImpl storageEngine = tableStore.getStorageEngine();
	    SchemaManager schemaManager = storageEngine.getSchemaManager();
	    NamespaceDescriptor namespaceDescriptor = schemaManager.getNamespace(name);
	    schemaManager.dropNamespace(namespaceDescriptor);
	    TableStoreSchema schema = tableStore.getSchema();
	    schema.removeNamespaceDefinition(name);
	    return null;
	} catch (SchemaException e) {
	    throw new ExecutionException("Could not drop namespace '" + name + "'.", e);
	}
    }

}
