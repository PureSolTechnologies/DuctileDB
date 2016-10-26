package com.puresoltechnologies.ductiledb.core.tables.dml;

/**
 * This class represents a single placeholder.
 * 
 * @author Rick-Rainer Ludwig
 */
public class Placeholder {

    private final int index;
    private final String columnFamily;
    private final String column;

    public Placeholder(int index, String columnFamily, String column) {
	super();
	if (index <= 0) {
	    throw new IllegalArgumentException("Column index has to be larger than zero.");
	}
	this.index = index;
	this.columnFamily = columnFamily;
	this.column = column;
    }

    public final int getIndex() {
	return index;
    }

    public final String getColumnFamily() {
	return columnFamily;
    }

    public final String getColumn() {
	return column;
    }

}
