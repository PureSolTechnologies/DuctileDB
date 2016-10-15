package com.puresoltechnologies.ductiledb.core.tables.ddl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.cf.index.secondary.IndexType;
import com.puresoltechnologies.ductiledb.storage.engine.cf.index.secondary.SecondaryIndexDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManager;
import com.puresoltechnologies.ductiledb.storage.engine.schema.TableDescriptor;

public class CreateIndexImpl implements CreateIndex {

    private final List<String> columns = new ArrayList<>();
    private final TableStoreImpl tableStore;
    private final String namespace;
    private final String table;
    private final String columnFamily;
    private final String name;

    public CreateIndexImpl(TableStoreImpl tableStore, String namespace, String table, String columnFamily,
	    String name) {
	super();
	this.tableStore = tableStore;
	this.namespace = namespace;
	this.table = table;
	this.columnFamily = columnFamily;
	this.name = name;
    }

    @Override
    public void addColumn(String column) {
	columns.add(column);
    }

    @Override
    public void execute() throws ExecutionException {
	if ("system".equals(namespace)) {
	    throw new ExecutionException("Creating tables in 'system' namespace is not allowed.");
	}
	try {
	    DatabaseEngineImpl storageEngine = tableStore.getStorageEngine();
	    SchemaManager schemaManager = storageEngine.getSchemaManager();
	    NamespaceDescriptor namespaceDescriptor = schemaManager.getNamespace(namespace);
	    TableDescriptor tableDescriptor = schemaManager.getTable(namespaceDescriptor, table);
	    ColumnFamilyDescriptor columnFamilyDescriptor = schemaManager.getColumnFamily(tableDescriptor,
		    Bytes.toBytes(columnFamily));

	    Set<byte[]> columns = new HashSet<>();
	    SecondaryIndexDescriptor indexDescriptor = new SecondaryIndexDescriptor(name, columnFamilyDescriptor,
		    columns, IndexType.HEAP);
	    schemaManager.createIndex(columnFamilyDescriptor, indexDescriptor);

	    tableStore.getSchema().addIndexDefinition(namespace, indexDescriptor);
	} catch (StorageException | SchemaException e) {
	    throw new ExecutionException("Could not create index '" + name + " in namespace " + namespace + "'.");
	}

    }

}
