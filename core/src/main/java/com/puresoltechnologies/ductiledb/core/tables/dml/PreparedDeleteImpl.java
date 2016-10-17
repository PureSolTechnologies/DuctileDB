package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.util.Map;

import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;

public class PreparedDeleteImpl extends AbstractPreparedWhereSelectionStatement implements PreparedDelete {

    public PreparedDeleteImpl(TableStoreImpl tableStore, String namespace, String table) {
	super(tableStore.getTableDefinition(namespace, table));
    }

    @Override
    public TableRowIterable execute(Map<String, Object> valueSpecifications) {
	// TODO Auto-generated method stub
	return null;
    }

}
