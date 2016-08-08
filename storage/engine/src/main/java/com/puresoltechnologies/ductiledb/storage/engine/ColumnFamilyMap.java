package com.puresoltechnologies.ductiledb.storage.engine;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.puresoltechnologies.ductiledb.storage.engine.utils.ByteArrayComparator;

public class ColumnFamilyMap {

    private final TreeMap<byte[], ColumnMap> columnFamilies = new TreeMap<>(ByteArrayComparator.getInstance());

    public ColumnMap get(byte[] columnFamily) {
	return columnFamilies.get(columnFamily);
    }

    public Set<byte[]> keySet() {
	return columnFamilies.keySet();
    }

    public void put(byte[] columnFamilyName, Map<byte[], byte[]> columnFamily) {
	columnFamilies.put(columnFamilyName, new ColumnMap(columnFamily));
    }

    public boolean isEmpty() {
	return columnFamilies.isEmpty();
    }
}
