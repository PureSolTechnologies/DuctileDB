package com.puresoltechnologies.ductiledb.core.tables.dcl;

import java.util.Map;

import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.TableStore;
import com.puresoltechnologies.ductiledb.core.tables.dml.TableRowIterable;

public class GrantImpl extends AbstractDCLStatement implements Grant {

    @Override
    public TableRowIterable execute(TableStore tableStore, Map<Integer, Comparable<?>> placeholderValue)
	    throws ExecutionException {
	// TODO Auto-generated method stub
	return null;
    }

}
