package com.puresoltechnologies.ductiledb.core.tables.ddl;

import java.util.Map;

import com.puresoltechnologies.ductiledb.core.tables.TableStore;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.dml.TableRowIterable;

public class DropIndexImpl extends AbstractDDLStatement implements DropIndex {

    private final TableStoreImpl tableStore;

    public DropIndexImpl(TableStoreImpl tableStore, String namespace, String table, String index) {
	super();
	this.tableStore = tableStore;
    }

    @Override
    public TableRowIterable execute(TableStore tableStore, Map<Integer, Comparable<?>> placeholderValue) {
	// TODO Auto-generated method stub
	return null;
    }

}
