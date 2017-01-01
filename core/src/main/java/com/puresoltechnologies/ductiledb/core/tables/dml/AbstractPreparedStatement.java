package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.util.Map;

import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;

public abstract class AbstractPreparedStatement implements PreparedStatement {

    private final TableStoreImpl tableStore;

    public AbstractPreparedStatement(TableStoreImpl tableStore) {
	super();
	this.tableStore = tableStore;
    }

    @Override
    public final BoundStatement bind() {
	return new BoundStatementImpl(this);
    }

    @Override
    public final BoundStatement bind(Object... values) {
	BoundStatementImpl boundStatement = new BoundStatementImpl(this);
	for (int i = 0; i < values.length; ++i) {
	    boundStatement.set(i + 1, values[i]);
	}
	return boundStatement;
    }

    public abstract TableRowIterable execute(Map<Integer, Comparable<?>> placeholderValue) throws ExecutionException;

    protected TableStoreImpl getTableStore() {
	return tableStore;
    }

}
