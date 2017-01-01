package com.puresoltechnologies.ductiledb.core.tables.ddl;

import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.dml.TableRowIterable;

public class CreateNamespaceIfNotExistsImpl extends CreateNamespaceImpl {

    public CreateNamespaceIfNotExistsImpl(TableStoreImpl storageEngine, String name) {
	super(storageEngine, name);
    }

    @Override
    public TableRowIterable execute() throws ExecutionException {
	// TODO
	return super.execute();
    }

}
