package com.puresoltechnologies.ductiledb.storage.engine;

import java.time.Instant;

import com.puresoltechnologies.ductiledb.storage.engine.index.RowKey;

/**
 * This class is used to keep the data of a single column family row.
 * 
 * @author Rick-Rainer Ludwig
 */
public final class ColumnFamilyRow {

    private final RowKey rowKey;
    private final Instant tombstone;
    private final ColumnMap columnMap;

    public ColumnFamilyRow(RowKey rowKey, ColumnMap columnMap) {
	this.rowKey = rowKey;
	this.columnMap = columnMap;
	this.tombstone = null;
    }

    public ColumnFamilyRow(RowKey rowKey, ColumnMap columnMap, Instant tombstone) {
	this.rowKey = rowKey;
	this.columnMap = columnMap;
	this.tombstone = tombstone;
    }

    public RowKey getRowKey() {
	return rowKey;
    }

    public Instant getTombstone() {
	return tombstone;
    }

    public ColumnMap getColumnMap() {
	return columnMap;
    }

    public boolean isEmpty() {
	return columnMap.isEmpty();
    }

    public boolean wasDeleted() {
	return tombstone != null;
    }

    public ColumnMap update(ColumnMap columnMap) {
	this.columnMap.putAll(columnMap);
	return columnMap;
    }

}
