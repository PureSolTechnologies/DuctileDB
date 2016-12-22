package com.puresoltechnologies.ductiledb.engine.cf;

import java.time.Instant;

import com.puresoltechnologies.ductiledb.engine.Key;
import com.puresoltechnologies.ductiledb.engine.io.Bytes;

/**
 * This class is used to keep the data of a single column family row.
 * 
 * @author Rick-Rainer Ludwig
 */
public final class ColumnFamilyRow {

    private final Key rowKey;
    private final Instant tombstone;
    private final ColumnMap columnMap;

    public ColumnFamilyRow(Key rowKey, ColumnMap columnMap) {
	this.rowKey = rowKey;
	this.columnMap = columnMap;
	this.tombstone = null;
    }

    public ColumnFamilyRow(Key rowKey, ColumnMap columnMap, Instant tombstone) {
	this.rowKey = rowKey;
	this.columnMap = columnMap;
	this.tombstone = tombstone;
    }

    public Key getRowKey() {
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

    @Override
    public String toString() {
	// TODO Auto-generated method stub
	return Bytes.toHumanReadableString(rowKey.getBytes()) + " " + (wasDeleted() ? "(deleted)" : "") + ":\n"
		+ columnMap.toString();
    }
}
