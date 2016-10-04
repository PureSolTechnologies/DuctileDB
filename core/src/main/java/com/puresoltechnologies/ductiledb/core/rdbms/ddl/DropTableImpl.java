package com.puresoltechnologies.ductiledb.core.rdbms.ddl;

import com.puresoltechnologies.ductiledb.api.rdbms.ddl.DropTable;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;

public class DropTableImpl implements DropTable {

    private final DatabaseEngineImpl storageEngine;

    public DropTableImpl(DatabaseEngineImpl storageEngine, String namespace, String table) {
	super();
	this.storageEngine = storageEngine;
    }

    @Override
    public void execute() {
	// TODO Auto-generated method stub

    }

}
