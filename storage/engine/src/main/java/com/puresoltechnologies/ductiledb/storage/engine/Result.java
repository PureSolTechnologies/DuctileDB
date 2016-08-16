package com.puresoltechnologies.ductiledb.storage.engine;

import java.util.NavigableMap;
import java.util.Set;

import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public class Result {

    private final byte[] rowKey;
    private final ColumnFamilyMap columnFamilies = new ColumnFamilyMap();

    public Result(byte[] rowKey) {
	this.rowKey = rowKey;
    }

    public Set<byte[]> getFamilies() {
	return columnFamilies.keySet();
    }

    public NavigableMap<byte[], byte[]> getFamilyMap(byte[] columnFamily) {
	return columnFamilies.get(columnFamily);
    }

    public boolean isEmpty() {
	return columnFamilies.isEmpty();
    }

    public byte[] getRowKey() {
	return rowKey;
    }

    public void add(byte[] columnFamily, ColumnMap columns) {
	if (!columns.isEmpty()) {
	    columnFamilies.put(columnFamily, columns);
	}
    }

    @Override
    public String toString() {
	StringBuilder buffer = new StringBuilder();
	buffer.append("Key: ");
	buffer.append(Bytes.toHumanReadableString(rowKey));
	buffer.append("\n");
	buffer.append(columnFamilies.toString());
	return buffer.toString();
    }
}
