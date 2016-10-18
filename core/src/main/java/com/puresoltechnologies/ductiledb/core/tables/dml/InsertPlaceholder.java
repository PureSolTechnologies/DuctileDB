package com.puresoltechnologies.ductiledb.core.tables.dml;

public class InsertPlaceholder {

    private final String columnFamily;
    private final String column;
    private final int index;

    public InsertPlaceholder(String columnFamily, String column, int index) {
	super();
	this.columnFamily = columnFamily;
	this.column = column;
	this.index = index;
    }

    public String getColumnFamily() {
	return columnFamily;
    }

    public String getColumn() {
	return column;
    }

    public int getIndex() {
	return index;
    }

}
