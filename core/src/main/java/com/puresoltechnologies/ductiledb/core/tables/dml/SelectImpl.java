package com.puresoltechnologies.ductiledb.core.tables.dml;

import com.puresoltechnologies.ductiledb.api.tables.dml.Select;
import com.puresoltechnologies.ductiledb.api.tables.dml.TableRowIterable;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.storage.engine.NamespaceEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.Scan;
import com.puresoltechnologies.ductiledb.storage.engine.TableEngineImpl;

public class SelectImpl implements Select {

    private final TableStoreImpl tableStore;
    private final String namespace;
    private final String table;
    private final NamespaceEngineImpl namespaceEngine;
    private final TableEngineImpl tableEngine;

    public SelectImpl(TableStoreImpl tableStore, String namespace, String table) {
	this.tableStore = tableStore;
	this.namespace = namespace;
	this.table = table;
	this.namespaceEngine = tableStore.getStorageEngine().getNamespaceEngine(namespace);
	this.tableEngine = namespaceEngine.getTableEngine(table);
    }

    @Override
    public TableRowIterable execute() {
	tableEngine.getScanner(new Scan());
	return null;
    }

}
