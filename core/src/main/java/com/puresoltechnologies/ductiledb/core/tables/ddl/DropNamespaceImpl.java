package com.puresoltechnologies.ductiledb.core.tables.ddl;

import com.puresoltechnologies.ductiledb.api.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.api.tables.ddl.DropNamespace;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.schema.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManager;

public class DropNamespaceImpl implements DropNamespace {

    private final TableStoreImpl tableStore;
    private final String name;

    public DropNamespaceImpl(TableStoreImpl tableStore, String name) {
	this.tableStore = tableStore;
	this.name = name;
    }

    @Override
    public void execute() throws ExecutionException {
	try {
	    DatabaseEngineImpl storageEngine = tableStore.getStorageEngine();
	    SchemaManager schemaManager = storageEngine.getSchemaManager();
	    NamespaceDescriptor namespaceDescriptor = schemaManager.getNamespace(name);
	    schemaManager.dropNamespace(namespaceDescriptor);
	} catch (SchemaException e) {
	    throw new ExecutionException("Could not drop namespace '" + name + "'.", e);
	}
    }

}
