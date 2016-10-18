package com.puresoltechnologies.ductiledb.core.tables.dml;

public class WhereClause {

    private final String columnFamily;
    private final String column;
    private final CompareOperator operator;
    private final Object value;

    public WhereClause(String columnFamily, String column, CompareOperator operator, Object value) {
	super();
	this.columnFamily = columnFamily;
	this.column = column;
	this.operator = operator;
	this.value = value;
    }

    public String getColumnFamily() {
	return columnFamily;
    }

    public String getColumn() {
	return column;
    }

    public CompareOperator getOperator() {
	return operator;
    }

    public Object getValue() {
	return value;
    }

}
