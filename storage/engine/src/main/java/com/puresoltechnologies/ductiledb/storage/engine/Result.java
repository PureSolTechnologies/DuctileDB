package com.puresoltechnologies.ductiledb.storage.engine;

import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyMap;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnMap;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.utils.ByteArrayComparator;

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
	TreeMap<byte[], byte[]> resultMap = new TreeMap<>(ByteArrayComparator.getInstance());
	ColumnMap columnMap = columnFamilies.get(columnFamily);
	if (columnMap == null) {
	    return null;
	}
	columnMap.forEach((key, value) -> resultMap.put(key, value.getValue()));
	return resultMap;
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
