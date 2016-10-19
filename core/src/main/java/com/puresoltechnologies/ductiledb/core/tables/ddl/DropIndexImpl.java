package com.puresoltechnologies.ductiledb.core.tables.ddl;

import com.puresoltechnologies.ductiledb.core.tables.TableStore;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.dml.TableRowIterable;

public class DropIndexImpl implements DropIndex {

    private final TableStoreImpl tableStore;

    public DropIndexImpl(TableStoreImpl tableStore, String namespace, String table, String index) {
	super();
	this.tableStore = tableStore;
    }

    @Override
    public TableRowIterable execute(TableStore tableStore) {
	// TODO Auto-generated method stub
	return null;
    }

}
