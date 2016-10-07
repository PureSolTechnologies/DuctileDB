package com.puresoltechnologies.ductiledb.core.tables.ddl;

import com.puresoltechnologies.ductiledb.api.tables.ddl.CreateIndex;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;

public class CreateIndexImpl implements CreateIndex {

    private final DatabaseEngineImpl storageEngine;

    public CreateIndexImpl(DatabaseEngineImpl storageEngine, String namespace, String table, String index) {
	super();
	this.storageEngine = storageEngine;
    }

    @Override
    public void execute() {
	// TODO Auto-generated method stub

    }

}
