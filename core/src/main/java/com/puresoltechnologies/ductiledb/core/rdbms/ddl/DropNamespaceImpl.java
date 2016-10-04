package com.puresoltechnologies.ductiledb.core.rdbms.ddl;

import com.puresoltechnologies.ductiledb.api.rdbms.ddl.DropNamespace;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;

public class DropNamespaceImpl implements DropNamespace {

    private final DatabaseEngineImpl storageEngine;

    public DropNamespaceImpl(DatabaseEngineImpl storageEngine, String namespace) {
	this.storageEngine = storageEngine;
    }

    @Override
    public void execute() {
	// TODO Auto-generated method stub

    }

}
