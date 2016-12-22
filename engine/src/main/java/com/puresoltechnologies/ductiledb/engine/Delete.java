package com.puresoltechnologies.ductiledb.engine;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Delete {

    private final Key key;
    private final Map<Key, Set<Key>> columnFamilies = new HashMap<>();

    public Delete(Key key) {
	super();
	this.key = key;
    }

    public final Key getKey() {
	return key;
    }

    public void addColumns(Key columnFamily, Key columnKey) {
	Set<Key> columns = columnFamilies.get(columnFamily);
	if (columns == null) {
	    columns = new HashSet<>();
	    columnFamilies.put(columnFamily, columns);
	}
	columns.add(columnKey);
	columnFamilies.put(columnFamily, columns);
    }

    public void addFamily(Key columnFamily) {
	Set<Key> columns = columnFamilies.get(columnFamily);
	if (columns == null) {
	    columns = new HashSet<>();
	    columnFamilies.put(columnFamily, columns);
	}
    }

    public Set<Key> getColumnFamilies() {
	return columnFamilies.keySet();
    }

    public Set<Key> getColumns(Key columnKey) {
	return columnFamilies.get(columnKey);
    }

}
