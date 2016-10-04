package com.puresoltechnologies.ductiledb.core.rdbms.ddl;

import com.puresoltechnologies.ductiledb.api.rdbms.ddl.CreateTable;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;

public class CreateTableImpl implements CreateTable {

    private final DatabaseEngineImpl storageEngine;

    public CreateTableImpl(DatabaseEngineImpl storageEngine, String namespace, String table) {
	super();
	this.storageEngine = storageEngine;
    }

    @Override
    public void execute() {
	// TODO Auto-generated method stub

    }

}
