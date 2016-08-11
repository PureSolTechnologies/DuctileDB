package com.puresoltechnologies.ductiledb.storage.engine;

import java.util.NavigableMap;
import java.util.Set;

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
	columnFamilies.put(columnFamily, columns);
    }

}
