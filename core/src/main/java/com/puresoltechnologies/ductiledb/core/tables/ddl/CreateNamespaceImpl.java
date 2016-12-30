package com.puresoltechnologies.ductiledb.core.tables.ddl;

import java.time.Instant;

import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.TableStore;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.dml.DataManipulationLanguage;
import com.puresoltechnologies.ductiledb.core.tables.dml.PreparedInsert;
import com.puresoltechnologies.ductiledb.core.tables.dml.TableRowIterable;
import com.puresoltechnologies.ductiledb.core.tables.schema.TableStoreSchema;
import com.puresoltechnologies.ductiledb.engine.DatabaseEngine;
import com.puresoltechnologies.ductiledb.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.engine.schema.SchemaManager;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;

public class CreateNamespaceImpl extends AbstractDDLStatement implements CreateNamespace {

    private final TableStoreImpl tableStore;
    private final String name;

    public CreateNamespaceImpl(TableStoreImpl storageEngine, String name) {
	this.tableStore = storageEngine;
	this.name = name;
    }

    @Override
    public TableRowIterable execute(TableStore tableStore) throws ExecutionException {
	if ("system".equals(name)) {
	    throw new ExecutionException("Creation of 'system' namespace is not allowed.");
	}
	try {
	    DatabaseEngine storageEngine = ((TableStoreImpl) tableStore).getStorageEngine();
	    SchemaManager schemaManager = storageEngine.getSchemaManager();
	    schemaManager.createNamespace(name);
	    TableStoreSchema schema = ((TableStoreImpl) tableStore).getSchema();
	    schema.addNamespaceDefinition(new NamespaceDefinitionImpl(name));
	    DataManipulationLanguage dml = tableStore.getDataManipulationLanguage();
	    PreparedInsert preparedInsert = dml.prepareInsert("system", "namespaces");
	    preparedInsert.addValue("metadata", "created", Instant.now());
	    preparedInsert.addValue("metadata", "created", Instant.now());
	    return null;
	} catch (StorageException | SchemaException e) {
	    throw new ExecutionException("Could not create namespace '" + name + "'.", e);
	}
    }

}
