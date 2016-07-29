package com.puresoltechnologies.ductiledb.storage.engine;

import java.util.NavigableMap;

import com.puresoltechnologies.ductiledb.storage.engine.memtable.ColumnFamilyMap;
import com.puresoltechnologies.ductiledb.storage.engine.memtable.ColumnMap;

public class Result {

    private final byte[] key;
    private final ColumnFamilyMap columnFamilies = new ColumnFamilyMap();

    public Result(byte[] rowKey) {
	this.key = rowKey;
    }

    public NavigableMap<byte[], byte[]> getFamilyMap(byte[] columnFamily) {
	return columnFamilies.get(columnFamily);
    }

    public boolean isEmpty() {
	return columnFamilies.isEmpty();
    }

    public byte[] getRow() {
	return key;
    }

    public void add(byte[] columnFamily, ColumnMap columns) {
	columnFamilies.put(columnFamily, columns);
    }

}
