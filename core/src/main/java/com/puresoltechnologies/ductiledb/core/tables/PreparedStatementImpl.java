package com.puresoltechnologies.ductiledb.core.tables;

import com.puresoltechnologies.ductiledb.core.tables.dml.TableRowIterable;

public abstract class PreparedStatementImpl implements PreparedStatement {

    public abstract TableRowIterable execute();

    @Override
    public final BoundStatement bind() {
	return new BoundStatementImpl(this);
    }

}
