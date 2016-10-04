package com.puresoltechnologies.ductiledb.core.rdbms.ddl;

import com.puresoltechnologies.ductiledb.api.rdbms.ddl.DropIndex;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;

public class DropIndexImpl implements DropIndex {
    private final DatabaseEngineImpl storageEngine;

    public DropIndexImpl(DatabaseEngineImpl storageEngine, String namespace, String table, String index) {
	super();
	this.storageEngine = storageEngine;
    }

    @Override
    public void execute() {
	// TODO Auto-generated method stub

    }

}
