package com.puresoltechnologies.ductiledb.core.tables.ddl;

import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.dml.TableRowIterable;
import com.puresoltechnologies.ductiledb.core.tables.schema.TableStoreSchema;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngine;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManager;

public class CreateNamespaceImpl implements CreateNamespace {

    private final TableStoreImpl tableStore;
    private final String name;

    public CreateNamespaceImpl(TableStoreImpl storageEngine, String name) {
	this.tableStore = storageEngine;
	this.name = name;
    }

    @Override
    public TableRowIterable execute() throws ExecutionException {
	if ("system".equals(name)) {
	    throw new ExecutionException("Creation of 'system' namespace is not allowed.");
	}
	try {
	    DatabaseEngine storageEngine = tableStore.getStorageEngine();
	    SchemaManager schemaManager = storageEngine.getSchemaManager();
	    schemaManager.createNamespace(name);
	    TableStoreSchema schema = tableStore.getSchema();
	    schema.addNamespaceDefinition(new NamespaceDefinitionImpl(name));
	    return null;
	} catch (StorageException | SchemaException e) {
	    throw new ExecutionException("Could not create namespace '" + name + "'.", e);
	}
    }

}
