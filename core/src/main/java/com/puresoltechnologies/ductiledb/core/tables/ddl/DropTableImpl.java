package com.puresoltechnologies.ductiledb.core.tables.ddl;

import java.util.Map;

import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.TableStore;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.dml.TableRowIterable;
import com.puresoltechnologies.ductiledb.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.engine.schema.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.engine.schema.SchemaManager;
import com.puresoltechnologies.ductiledb.engine.schema.TableDescriptor;

public class DropTableImpl extends AbstractDDLStatement implements DropTable {

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
    public TableRowIterable execute(TableStore tableStore, Map<Integer, Comparable<?>> placeholderValue)
	    throws ExecutionException {
	if ("system".equals(namespace)) {
	    throw new ExecutionException("Dropping tables from 'system' namespace is not allowed.");
	}
	try {
	    DatabaseEngineImpl storageEngine = ((TableStoreImpl) tableStore).getStorageEngine();
	    SchemaManager schemaManager = storageEngine.getSchemaManager();
	    NamespaceDescriptor namespaceDescriptor = schemaManager.getNamespace(namespace);
	    TableDescriptor tableDescriptor = schemaManager.getTable(namespaceDescriptor, name);
	    schemaManager.dropTable(tableDescriptor);
	    ((TableStoreImpl) tableStore).getSchema().removeTableDefinition(namespace, tableDescriptor.getName());
	    return null;
	} catch (SchemaException e) {
	    throw new ExecutionException("Could not drop table '" + namespace + "." + name + "'.", e);
	}
    }

}
