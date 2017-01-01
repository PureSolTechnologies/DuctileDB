package com.puresoltechnologies.ductiledb.core.tables.ddl;

import com.puresoltechnologies.ductiledb.core.tables.StatementImpl;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;

public abstract class AbstractDDLStatement extends StatementImpl {

    public AbstractDDLStatement(TableStoreImpl tableStore) {
	super(tableStore);
    }

}
