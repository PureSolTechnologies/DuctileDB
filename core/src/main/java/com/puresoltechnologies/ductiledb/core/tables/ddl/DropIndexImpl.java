package com.puresoltechnologies.ductiledb.core.tables.ddl;

import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.dml.TableRowIterable;

public class DropIndexImpl extends AbstractDDLStatement implements DropIndex {

    public DropIndexImpl(TableStoreImpl tableStore, String namespace, String table, String index) {
	super(tableStore);
    }

    @Override
    public TableRowIterable execute() {
	// TODO Auto-generated method stub
	return null;
    }

}
