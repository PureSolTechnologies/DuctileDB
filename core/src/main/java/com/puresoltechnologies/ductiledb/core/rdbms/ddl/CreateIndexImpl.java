package com.puresoltechnologies.ductiledb.core.rdbms.ddl;

import com.puresoltechnologies.ductiledb.api.rdbms.ddl.CreateIndex;
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
