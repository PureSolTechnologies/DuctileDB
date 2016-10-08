package com.puresoltechnologies.ductiledb.core.tables.ddl;

import com.puresoltechnologies.ductiledb.api.tables.ValueTypes;
import com.puresoltechnologies.ductiledb.api.tables.ddl.CreateTable;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;

public class CreateTableImpl implements CreateTable {

    private final TableStoreImpl tableStore;

    public CreateTableImpl(TableStoreImpl tableStore, String namespace, String table) {
	super();
	this.tableStore = tableStore;
    }

    @Override
    public void execute() {
	// TODO Auto-generated method stub

    }

    @Override
    public void addColumn(String string, ValueTypes dateTime) {
	// TODO Auto-generated method stub

    }

    @Override
    public void setPrimaryKey(String... columns) {
	// TODO Auto-generated method stub

    }

}
