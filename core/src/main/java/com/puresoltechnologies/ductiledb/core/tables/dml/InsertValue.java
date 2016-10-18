package com.puresoltechnologies.ductiledb.core.tables.dml;

public class InsertValue {

    private final String columnFamily;
    private final String column;
    private final Object value;

    public InsertValue(String columnFamily, String column, Object value) {
	super();
	this.columnFamily = columnFamily;
	this.column = column;
	this.value = value;
    }

    public String getColumnFamily() {
	return columnFamily;
    }

    public String getColumn() {
	return column;
    }

    public Object getValue() {
	return value;
    }

}
