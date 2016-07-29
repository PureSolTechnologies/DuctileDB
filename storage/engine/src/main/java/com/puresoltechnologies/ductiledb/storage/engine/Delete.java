package com.puresoltechnologies.ductiledb.storage.engine;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Delete {

    private final byte[] key;
    private final Map<byte[], Set<byte[]>> columnFamilies = new HashMap<>();

    public Delete(byte[] key) {
	super();
	this.key = key;
    }

    public final byte[] getKey() {
	return key;
    }

    public void addColumns(byte[] columnFamily, byte[] columnKey) {
	Set<byte[]> columns = columnFamilies.get(columnFamily);
	if (columns == null) {
	    columns = new HashSet<>();
	    columnFamilies.put(columnFamily, columns);
	}
	columns.add(columnKey);
	columnFamilies.put(columnFamily, columns);
    }

    public void addFamily(byte[] columnFamily) {
	Set<byte[]> columns = columnFamilies.get(columnFamily);
	if (columns == null) {
	    columns = new HashSet<>();
	    columnFamilies.put(columnFamily, columns);
	}
    }

    public Set<byte[]> getColumnFamilies() {
	return columnFamilies.keySet();
    }

    public Set<byte[]> getColumns(byte[] columnKey) {
	return columnFamilies.get(columnKey);
    }

}
