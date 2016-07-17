package com.puresoltechnologies.ductiledb.storage.engine;

import com.puresoltechnologies.ductiledb.storage.engine.utils.ArrayComparator;

public class ColumnEntry implements Comparable<ColumnEntry> {

    private static final ArrayComparator comparator = new ArrayComparator();

    private final byte[] key;
    private final byte[] value;

    public ColumnEntry(byte[] key, byte[] value) {
	super();
	this.key = key;
	this.value = value;
    }

    public final byte[] getKey() {
	return key;
    }

    public final byte[] getValue() {
	return value;
    }

    @Override
    public int compareTo(ColumnEntry o) {
	return comparator.compare(key, o.key);
    }

}
