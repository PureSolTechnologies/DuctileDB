package com.puresoltechnologies.ductiledb.core.tables.ddl;

import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.dml.TableRowIterable;

public class CreateTableIfNotExistsImpl extends CreateTableImpl {

    public CreateTableIfNotExistsImpl(TableStoreImpl tableStore, String namespace, String name, String description) {
	super(tableStore, namespace, name, description);
    }

    @Override
    public TableRowIterable execute() throws ExecutionException {
	// TODO Auto-generated method stub
	return super.execute();
    }
}
