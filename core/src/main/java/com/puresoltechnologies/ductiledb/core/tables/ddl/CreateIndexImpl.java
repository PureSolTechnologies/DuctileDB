package com.puresoltechnologies.ductiledb.core.tables.ddl;

import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;

public class CreateIndexImpl implements CreateIndex {

    private final TableStoreImpl tableStore;

    public CreateIndexImpl(TableStoreImpl tableStore, String namespace, String table, String index) {
	super();
	this.tableStore = tableStore;
    }

    @Override
    public void execute() {
	// TODO Auto-generated method stub

    }

}
