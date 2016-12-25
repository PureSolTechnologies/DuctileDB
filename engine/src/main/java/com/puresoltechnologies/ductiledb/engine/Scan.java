package com.puresoltechnologies.ductiledb.engine;

import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.puresoltechnologies.ductiledb.logstore.Key;

public class Scan {

    private final Key startRow;
    private final Key endRow;
    private NavigableMap<Key, NavigableSet<Key>> columnFamilies = new TreeMap<>();

    public Scan() {
	this(null, null);
    }

    public Scan(Key startRow) {
	this(startRow, null);
    }

    public Scan(Key startRow, Key endRow) {
	super();
	this.startRow = startRow;
	this.endRow = endRow;
    }

    public Key getStartRow() {
	return startRow;
    }

    public Key getEndRow() {
	return endRow;
    }

    public void addFamily(Key columnFamily) {
	columnFamilies.remove(columnFamily);
	columnFamilies.put(columnFamily, null);
    }

    public void addColumn(Key columnFamily, Key column) {
	NavigableSet<Key> columns = columnFamilies.get(columnFamily);
	if (columns == null) {
	    columns = new TreeSet<>();
	    columnFamilies.put(columnFamily, columns);
	}
	columns.add(column);
    }

    public NavigableMap<Key, NavigableSet<Key>> getColumnFamilies() {
	return columnFamilies;
    }
}
