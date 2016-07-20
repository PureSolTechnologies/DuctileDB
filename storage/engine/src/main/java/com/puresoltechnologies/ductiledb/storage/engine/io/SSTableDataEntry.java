package com.puresoltechnologies.ductiledb.storage.engine.io;

import com.puresoltechnologies.ductiledb.storage.engine.memtable.ColumnMap;

public class SSTableDataEntry {

    private final byte[] rowKey;
    private final ColumnMap columns = new ColumnMap();

    public SSTableDataEntry(byte[] rowKey, ColumnMap columns) {
	super();
	this.rowKey = rowKey;
	this.columns.putAll(columns);
    }

    public byte[] getRowKey() {
	return rowKey;
    }

    public ColumnMap getColumns() {
	return columns;
    }

}
