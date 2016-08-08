package com.puresoltechnologies.ductiledb.storage.engine;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Put {

    private final ColumnFamilyMap columnFamilies = new ColumnFamilyMap();
    private final byte[] key;

    public Put(byte[] key) {
	super();
	this.key = key;
    }

    public final byte[] getKey() {
	return key;
    }

    public final Set<byte[]> getColumnFamilies() {
	return columnFamilies.keySet();
    }

    public final ColumnMap getColumnValues(byte[] columnFamily) {
	return columnFamilies.get(columnFamily);
    }

    public final void addColumn(byte[] columnFamilyName, byte[] key, byte[] value) {
	Map<byte[], byte[]> columnFamily = columnFamilies.get(columnFamilyName);
	if (columnFamily == null) {
	    columnFamily = new HashMap<>();
	    columnFamilies.put(columnFamilyName, columnFamily);
	}
	columnFamily.put(key, value);
    }

}
