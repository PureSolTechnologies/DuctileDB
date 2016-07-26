package com.puresoltechnologies.ductiledb.storage.engine.io.sstable;

import com.puresoltechnologies.ductiledb.storage.engine.memtable.ColumnMap;

/**
 * This class is used to keep the data of a single column family row.
 * 
 * @author Rick-Rainer Ludwig
 */
public class ColumnFamilyRow {

    private final byte[] rowKey;
    private final ColumnMap columnMap;

    public ColumnFamilyRow(byte[] rowKey, ColumnMap columnMap) {
	this.rowKey = rowKey;
	this.columnMap = columnMap;
    }

    public byte[] getRowKey() {
	return rowKey;
    }

    public ColumnMap getColumnMap() {
	return columnMap;
    }

    public ColumnMap update(ColumnMap columnMap) {
	this.columnMap.putAll(columnMap);
	return columnMap;
    }

}
