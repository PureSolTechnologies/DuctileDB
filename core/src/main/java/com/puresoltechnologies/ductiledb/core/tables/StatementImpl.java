package com.puresoltechnologies.ductiledb.core.tables;

public abstract class StatementImpl implements Statement {

    private final TableStoreImpl tableStore;

    public StatementImpl(TableStoreImpl tableStore) {
	super();
	this.tableStore = tableStore;
    }

    protected TableStoreImpl getTableStore() {
	return tableStore;
    }

}
