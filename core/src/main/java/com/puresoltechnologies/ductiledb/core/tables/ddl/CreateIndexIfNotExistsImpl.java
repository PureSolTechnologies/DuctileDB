package com.puresoltechnologies.ductiledb.core.tables.ddl;

import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.dml.TableRowIterable;

public class CreateIndexIfNotExistsImpl extends CreateIndexImpl {

    public CreateIndexIfNotExistsImpl(TableStoreImpl tableStore, String namespace, String table, String columnFamily,
	    String name) {
	super(tableStore, namespace, table, columnFamily, name);
    }

    @Override
    public TableRowIterable execute() throws ExecutionException {
	// TODO
	return super.execute();
    }
}
