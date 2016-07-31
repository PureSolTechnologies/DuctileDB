package com.puresoltechnologies.ductiledb.storage.engine;

import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.puresoltechnologies.ductiledb.storage.engine.utils.ByteArrayComparator;

public class Scan {

    private final byte[] startRow;
    private final byte[] endRow;
    private NavigableMap<byte[], NavigableSet<byte[]>> columnFamilies = new TreeMap<>(
	    ByteArrayComparator.getInstance());

    public Scan() {
	this(null, null);
    }

    public Scan(byte[] startRow) {
	this(startRow, null);
    }

    public Scan(byte[] startRow, byte[] endRow) {
	super();
	this.startRow = startRow;
	this.endRow = endRow;
    }

    public byte[] getStartRow() {
	return startRow;
    }

    public byte[] getEndRow() {
	return endRow;
    }

    public void addFamily(byte[] columnFamily) {
	columnFamilies.remove(columnFamily);
	columnFamilies.put(columnFamily, null);
    }

    public void addColumn(byte[] columnFamily, byte[] column) {
	NavigableSet<byte[]> columns = columnFamilies.get(columnFamily);
	if (columns == null) {
	    columns = new TreeSet<>();
	    columnFamilies.put(columnFamily, columns);
	}
	columns.add(column);
    }

    public NavigableMap<byte[], NavigableSet<byte[]>> getColumnFamilies() {
	return columnFamilies;
    }
}
