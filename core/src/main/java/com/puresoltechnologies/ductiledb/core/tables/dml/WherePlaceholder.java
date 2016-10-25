package com.puresoltechnologies.ductiledb.core.tables.dml;

public class WherePlaceholder extends Placeholder {

    private final CompareOperator operator;

    public WherePlaceholder(int index, String columnFamily, String column, CompareOperator operator) {
	super(index, columnFamily, column);
	this.operator = operator;
    }

    public CompareOperator getOperator() {
	return operator;
    }

}
