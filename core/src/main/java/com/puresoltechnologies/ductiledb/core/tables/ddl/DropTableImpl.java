package com.puresoltechnologies.ductiledb.core.tables.ddl;

import com.puresoltechnologies.ductiledb.api.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.api.tables.ddl.DropTable;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
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
    public void execute() throws ExecutionException {
	try {
	    DatabaseEngineImpl storageEngine = tableStore.getStorageEngine();
	    SchemaManager schemaManager = storageEngine.getSchemaManager();
	    NamespaceDescriptor namespaceDescriptor = schemaManager.getNamespace(namespace);
	    TableDescriptor tableDescriptor = schemaManager.getTable(namespaceDescriptor, name);
	    schemaManager.dropTable(tableDescriptor);
	} catch (SchemaException e) {
	    throw new ExecutionException("Could not drop table '" + namespace + "." + name + "'.", e);
	}
    }

}
