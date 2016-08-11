package com.puresoltechnologies.ductiledb.storage.engine;

import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.utils.ByteArrayComparator;

public class ColumnFamilyMap {

    private final TreeMap<byte[], ColumnMap> columnFamilies = new TreeMap<>(ByteArrayComparator.getInstance());

    public ColumnMap get(byte[] columnFamily) {
	return columnFamilies.get(columnFamily);
    }

    public Set<byte[]> keySet() {
	return columnFamilies.keySet();
    }

    public void put(byte[] columnFamilyName, ColumnMap columnMap) {
	columnFamilies.put(columnFamilyName, columnMap);
    }

    public boolean isEmpty() {
	return columnFamilies.isEmpty();
    }

    @Override
    public String toString() {
	StringBuilder buffer = new StringBuilder();
	for (Entry<byte[], ColumnMap> columnFamily : columnFamilies.entrySet()) {
	    if (buffer.length() > 0) {
		buffer.append('\n');
	    }
	    buffer.append("family: ");
	    buffer.append(Bytes.toHumanReadableString(columnFamily.getKey()));
	    buffer.append("\n  ");
	    buffer.append(columnFamily.getValue().toString().replaceAll("\\n", "\n    "));
	}
	return buffer.toString();
    }
}
