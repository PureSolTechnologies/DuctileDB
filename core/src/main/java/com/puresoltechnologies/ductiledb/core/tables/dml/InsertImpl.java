package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.util.HashMap;
import java.util.Map;

import com.puresoltechnologies.ductiledb.api.tables.ddl.TableDefinition;
import com.puresoltechnologies.ductiledb.api.tables.dml.Insert;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.NamespaceEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.Put;
import com.puresoltechnologies.ductiledb.storage.engine.TableEngineImpl;

public class InsertImpl implements Insert {

    private final TableStoreImpl tableStore;
    private final String namespace;
    private final String table;
    private final Map<String, Map<String, Object>> values = new HashMap<>();

    public InsertImpl(TableStoreImpl tableStore, String namespace, String table) {
	this.tableStore = tableStore;
	this.namespace = namespace;
	this.table = table;
    }

    @Override
    public void addValue(String columnFamily, String column, Object value) {
	Map<String, Object> cf = values.get(columnFamily);
	if (cf == null) {
	    cf = new HashMap<>();
	    values.put(columnFamily, cf);
	}
	cf.put(column, value);
    }

    @Override
    public void execute() {
	TableDefinition tableDefinition = tableStore.getTableDefinition(namespace, table);
	DatabaseEngineImpl storageEngine = tableStore.getStorageEngine();
	NamespaceEngineImpl namespaceEngine = storageEngine.getNamespaceEngine(namespace);
	TableEngineImpl tableEngine = namespaceEngine.getTableEngine(table);
	Put put = new Put(null);
	tableEngine.put(put);
    }

}
