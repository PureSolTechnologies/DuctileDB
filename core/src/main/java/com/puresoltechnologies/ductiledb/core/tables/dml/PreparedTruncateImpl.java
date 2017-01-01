package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.util.Map;

import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinition;

public class PreparedTruncateImpl extends AbstractPreparedDMLStatement implements PreparedTruncate {

    public PreparedTruncateImpl(TableStoreImpl tableStore, TableDefinition tableDefinition) {
	super(tableStore, tableDefinition);
    }

    @Override
    public TableRowIterable execute(Map<Integer, Comparable<?>> placeholderValue) {
	// TODO Auto-generated method stub
	return null;
    }

}
