package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.util.Map;

import com.puresoltechnologies.ductiledb.core.tables.TableStore;
import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinition;

public class PreparedDeleteImpl extends AbstractPreparedWhereSelectionStatement implements PreparedDelete {

    public PreparedDeleteImpl(TableDefinition tableDefinition) {
	super(tableDefinition);
    }

    @Override
    public TableRowIterable execute(TableStore tableStore, Map<String, Object> valueSpecifications) {
	// TODO Auto-generated method stub
	return null;
    }

}
