package com.puresoltechnologies.ductiledb.engine;

import java.util.Set;

import com.puresoltechnologies.ductiledb.engine.cf.ColumnFamilyMap;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnMap;
import com.puresoltechnologies.ductiledb.logstore.Key;

public class Result {

    private final Key rowKey;
    private final ColumnFamilyMap columnFamilies = new ColumnFamilyMap();

    public Result(Key rowKey) {
	this.rowKey = rowKey;
    }

    public Set<Key> getFamilies() {
	return columnFamilies.keySet();
    }

    public ColumnMap getFamilyMap(Key columnFamily) {
	ColumnMap resultMap = new ColumnMap();
	ColumnMap columnMap = columnFamilies.get(columnFamily);
	if (columnMap == null) {
	    return null;
	}
	columnMap.forEach((key, value) -> resultMap.put(key, value));
	return resultMap;
    }

    public boolean isEmpty() {
	return columnFamilies.isEmpty();
    }

    public Key getRowKey() {
	return rowKey;
    }

    public void add(Key columnFamily, ColumnMap columns) {
	if (!columns.isEmpty()) {
	    columnFamilies.put(columnFamily, columns);
	}
    }

    @Override
    public String toString() {
	StringBuilder buffer = new StringBuilder();
	buffer.append("Key: ");
	buffer.append(rowKey);
	buffer.append("\n");
	buffer.append(columnFamilies.toString());
	return buffer.toString();
    }
}
