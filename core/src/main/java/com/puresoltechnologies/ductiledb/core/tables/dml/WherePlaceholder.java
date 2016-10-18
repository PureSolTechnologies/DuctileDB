package com.puresoltechnologies.ductiledb.core.tables.dml;

public class WherePlaceholder {

    private final String columnFamily;
    private final String column;
    private final CompareOperator operator;
    private final int index;

    public WherePlaceholder(String columnFamily, String column, CompareOperator operator, int index) {
	super();
	this.columnFamily = columnFamily;
	this.column = column;
	this.operator = operator;
	this.index = index;
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

    public int getIndex() {
	return index;
    }

}
