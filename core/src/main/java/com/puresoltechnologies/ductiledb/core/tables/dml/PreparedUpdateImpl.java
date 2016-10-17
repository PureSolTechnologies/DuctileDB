package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.util.Map;

import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;

public class PreparedUpdateImpl extends AbstractPreparedWhereSelectionStatement implements PreparedUpdate {

    public PreparedUpdateImpl(TableStoreImpl tableStore, String namespace, String table) {
	super(tableStore.getTableDefinition(namespace, table));
    }

    @Override
    public TableRowIterable execute(Map<String, Object> valueSpecifications) {
	// TODO Auto-generated method stub
	return null;
    }

}
