package com.puresoltechnologies.ductiledb.storage.engine;

import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.puresoltechnologies.ductiledb.storage.engine.utils.ByteArrayComparator;

public class Get {

    private final byte[] key;
    private final NavigableMap<byte[], NavigableSet<byte[]>> columnFamilies = new TreeMap<>(
	    ByteArrayComparator.getInstance());

    public Get(byte[] key) {
	super();
	this.key = key;
    }

    public final byte[] getKey() {
	return key;
    }

    public final void addFamily(byte[] columnFamily) {
	columnFamilies.remove(columnFamily);
	columnFamilies.put(columnFamily, null);
    }

    public final void addColumn(byte[] columnFamily, byte[] column) {
	NavigableSet<byte[]> columns = columnFamilies.get(columnFamily);
	if (columns == null) {
	    columns = new TreeSet<>();
	    columnFamilies.put(columnFamily, columns);
	}
	columns.add(column);
    }

    public final NavigableMap<byte[], NavigableSet<byte[]>> getColumnFamilies() {
	return columnFamilies;
    }

}
