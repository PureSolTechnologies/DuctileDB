package com.puresoltechnologies.ductiledb.engine;

import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

public class Get {

    private final Key key;
    private final NavigableMap<Key, NavigableSet<Key>> columnFamilies = new TreeMap<>();

    public Get(Key key) {
	super();
	this.key = key;
    }

    public final Key getKey() {
	return key;
    }

    public final void addFamily(Key columnFamily) {
	columnFamilies.remove(columnFamily);
	columnFamilies.put(columnFamily, null);
    }

    public final void addColumn(Key columnFamily, Key column) {
	NavigableSet<Key> columns = columnFamilies.get(columnFamily);
	if (columns == null) {
	    columns = new TreeSet<>();
	    columnFamilies.put(columnFamily, columns);
	}
	columns.add(column);
    }

    public final NavigableMap<Key, NavigableSet<Key>> getColumnFamilies() {
	return columnFamilies;
    }

}
