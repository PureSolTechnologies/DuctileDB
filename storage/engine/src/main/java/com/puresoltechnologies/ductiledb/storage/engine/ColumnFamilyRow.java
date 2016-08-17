package com.puresoltechnologies.ductiledb.storage.engine;

import com.puresoltechnologies.ductiledb.storage.engine.index.RowKey;

/**
 * This class is used to keep the data of a single column family row.
 * 
 * @author Rick-Rainer Ludwig
 */
public class ColumnFamilyRow {

    private final RowKey rowKey;
    private final ColumnMap columnMap;

    public ColumnFamilyRow(RowKey rowKey, ColumnMap columnMap) {
	this.rowKey = rowKey;
	this.columnMap = columnMap;
    }

    public RowKey getRowKey() {
	return rowKey;
    }

    public ColumnMap getColumnMap() {
	return columnMap;
    }

    public boolean isEmpty() {
	return columnMap.isEmpty();
    }

    public ColumnMap update(ColumnMap columnMap) {
	this.columnMap.putAll(columnMap);
	return columnMap;
    }

}
