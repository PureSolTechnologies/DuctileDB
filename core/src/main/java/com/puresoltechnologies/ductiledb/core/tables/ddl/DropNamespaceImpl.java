package com.puresoltechnologies.ductiledb.core.tables.ddl;

import com.puresoltechnologies.ductiledb.api.tables.ddl.DropNamespace;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;

public class DropNamespaceImpl implements DropNamespace {

    private final TableStoreImpl tableStore;

    public DropNamespaceImpl(TableStoreImpl tableStore, String namespace) {
	this.tableStore = tableStore;
    }

    @Override
    public void execute() {
	// TODO Auto-generated method stub

    }

}
