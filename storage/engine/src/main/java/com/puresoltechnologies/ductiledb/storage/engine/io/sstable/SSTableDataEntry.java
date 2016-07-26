package com.puresoltechnologies.ductiledb.storage.engine.io.sstable;

import java.util.Map.Entry;

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

    public ColumnMap update(ColumnMap columnMap) {
	ColumnMap updated = new ColumnMap();
	updated.putAll(columns);
	for (Entry<byte[], byte[]> entry : columnMap.entrySet()) {
	    byte[] value = entry.getValue();
	    if (value != null) {
		updated.put(entry.getKey(), value);
	    } else {
		updated.remove(entry.getKey());
	    }
	}
	return updated;
    }

}