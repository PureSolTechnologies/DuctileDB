package com.puresoltechnologies.ductiledb.core.tables.ddl;

import java.util.Map;

import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.TableStore;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.dml.TableRowIterable;
import com.puresoltechnologies.ductiledb.core.tables.schema.TableStoreSchema;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.schema.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManager;

public class DropNamespaceImpl extends AbstractDDLStatement implements DropNamespace {

    private final TableStoreImpl tableStore;
    private final String name;

    public DropNamespaceImpl(TableStoreImpl tableStore, String name) {
	this.tableStore = tableStore;
	this.name = name;
    }

    @Override
    public TableRowIterable execute(TableStore tableStore, Map<Integer, Comparable<?>> placeholderValue)
	    throws ExecutionException {
	if ("system".equals(name)) {
	    throw new ExecutionException("Dropping of 'system' namespace is not allowed.");
	}
	try {
	    DatabaseEngineImpl storageEngine = ((TableStoreImpl) tableStore).getStorageEngine();
	    SchemaManager schemaManager = storageEngine.getSchemaManager();
	    NamespaceDescriptor namespaceDescriptor = schemaManager.getNamespace(name);
	    schemaManager.dropNamespace(namespaceDescriptor);
	    TableStoreSchema schema = ((TableStoreImpl) tableStore).getSchema();
	    schema.removeNamespaceDefinition(name);
	    return null;
	} catch (SchemaException e) {
	    throw new ExecutionException("Could not drop namespace '" + name + "'.", e);
	}
    }

}
