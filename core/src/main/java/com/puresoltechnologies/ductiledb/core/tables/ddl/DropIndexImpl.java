package com.puresoltechnologies.ductiledb.core.tables.ddl;

import com.puresoltechnologies.ductiledb.api.tables.ddl.DropIndex;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;

public class DropIndexImpl implements DropIndex {

    private final TableStoreImpl tableStore;

    public DropIndexImpl(TableStoreImpl tableStore, String namespace, String table, String index) {
	super();
	this.tableStore = tableStore;
    }

    @Override
    public void execute() {
	// TODO Auto-generated method stub

    }

}
