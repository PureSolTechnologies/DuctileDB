package com.puresoltechnologies.ductiledb.core.tables;

import java.util.Map;

import com.puresoltechnologies.ductiledb.core.tables.dml.TableRowIterable;

public abstract class AbstractPreparedStatement implements PreparedStatement {

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

    public abstract TableRowIterable execute(TableStore tableStore, Map<Integer, Comparable<?>> placeholderValue)
	    throws ExecutionException;

}
