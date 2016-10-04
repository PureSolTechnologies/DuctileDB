package com.puresoltechnologies.ductiledb.core.rdbms.dml;

import com.puresoltechnologies.ductiledb.api.rdbms.dml.Select;
import com.puresoltechnologies.ductiledb.api.rdbms.dml.TableRowIterable;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngine;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.NamespaceEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.TableEngineImpl;

public class SelectImpl implements Select {

    private final DatabaseEngine databaseEngine;
    private final String namespace;
    private final String table;
    private final NamespaceEngineImpl namespaceEngine;
    private final TableEngineImpl tableEngine;

    public SelectImpl(DatabaseEngineImpl databaseEngine, String namespace, String table) {
	this.databaseEngine = databaseEngine;
	this.namespace = namespace;
	this.table = table;
	this.namespaceEngine = databaseEngine.getNamespaceEngine(namespace);
	this.tableEngine = namespaceEngine.getTableEngine(table);
    }

    @Override
    public TableRowIterable execute() {
	// TODO Auto-generated method stub
	return null;
    }

}
