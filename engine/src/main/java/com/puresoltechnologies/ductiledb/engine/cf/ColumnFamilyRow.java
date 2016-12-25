package com.puresoltechnologies.ductiledb.engine.cf;

import java.io.IOException;
import java.time.Instant;
import java.util.Map.Entry;
import java.util.Set;

import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.logstore.io.DataInputStream;
import com.puresoltechnologies.ductiledb.logstore.io.DataOutputStream;
import com.puresoltechnologies.ductiledb.logstore.utils.Bytes;

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

    @Override
    public void writeTo(DataOutputStream outputStream) {
	Set<Entry<Key, ColumnValue>> entrySet = entrySet();
	outputStream.writeData(Bytes.toBytes(entrySet.size()));
	// Columns
	for (Entry<Key, ColumnValue> column : entrySet) {
	    // Column key
	    Key columnKey = column.getKey();
	    outputStream.writeData(Bytes.toBytes(columnKey.getBytes().length));
	    outputStream.writeData(columnKey.getBytes());
	    // Column value
	    ColumnValue columnValue = column.getValue();
	    outputStream.writeTombstone(columnValue.getTombstone());
	    byte[] value = columnValue.getBytes();
	    outputStream.writeData(Bytes.toBytes(value.length));
	    if (value.length > 0) {
		outputStream.writeData(value);
	    }
	}
    }

    @Override
    public void readFrom(DataInputStream inputStream, Key rowKey, Instant tombstone) {
	byte[] buffer = new byte[12];
	// Read column count
	int len = inputStream.read(buffer, 0, 4);
	if (len < 4) {
	    throw new IOException("Could not read full number of bytes needed. It is maybe a broken data file.");
	}
	int columnCount = Bytes.toInt(buffer);
	// Read columns
	ColumnMap columns = new ColumnMap();
	for (int i = 0; i < columnCount; ++i) {
	    // Read column key...
	    len = inputStream.read(buffer, 0, 4);
	    if (len < 4) {
		throw new IOException("Could not read full number of bytes needed. It is maybe a broken data file.");
	    }
	    int length = Bytes.toInt(buffer);
	    byte[] columnKey = new byte[length];
	    len = inputStream.read(columnKey);
	    if (len < length) {
		throw new IOException("Could not read full number of bytes needed. It is maybe a broken data file.");
	    }
	    // Read column tombstone...
	    len = inputStream.read(buffer, 0, 12);
	    if (len < 12) {
		throw new IOException("Could not read full number of bytes needed. It is maybe a broken data file.");
	    }
	    Instant columnTombstone = Bytes.toTombstone(buffer);
	    // Read column value...
	    len = inputStream.read(buffer, 0, 4);
	    if (len < 4) {
		throw new IOException("Could not read full number of bytes needed. It is maybe a broken data file.");
	    }
	    length = Bytes.toInt(buffer);
	    byte[] columnValue = new byte[length];
	    if (length > 0) {
		len = inputStream.read(columnValue);
		if (len < length) {
		    throw new IOException(
			    "Could not read full number of bytes needed. It is maybe a broken data file.");
		}
	    }
	    columns.put(Key.of(columnKey), ColumnValue.of(columnValue, columnTombstone));
	}
	return new ColumnFamilyRow(rowKey, columns, tombstone);

    }
}
