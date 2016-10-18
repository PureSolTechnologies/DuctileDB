package com.puresoltechnologies.ductiledb.core.tables.dml;

import java.util.Map;

import com.puresoltechnologies.ductiledb.core.tables.ddl.TableDefinition;

public abstract class AbstractPreparedStatementImpl implements PreparedStatement {

    private final TableDefinition tableDefinition;

    public AbstractPreparedStatementImpl(TableDefinition tableDefinition) {
	super();
	this.tableDefinition = tableDefinition;
    }

    public final TableDefinition getTableDefinition() {
	return tableDefinition;
    }

    public abstract TableRowIterable execute(Map<String, Object> valueSpecifications);

    @Override
    public final BoundStatement bind() {
	return new BoundStatementImpl(this);
    }

    @Override
    public BoundStatement bind(Object... values) {
	BoundStatementImpl boundStatement = new BoundStatementImpl(this);
	for (int i = 0; i < values.length; ++i) {
	    boundStatement.set(i + 1, values[i]);
	}
	return boundStatement;
    }

}
