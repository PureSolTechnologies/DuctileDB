package com.puresoltechnologies.ductiledb.storage.engine.memtable;

import java.util.TreeMap;

import com.puresoltechnologies.ductiledb.storage.engine.utils.ByteArrayComparator;

/**
 * Basic Memtable implementation.
 * 
 * @author Rick-Rainer Ludwig
 */
public class Memtable {

    private final TreeMap<byte[], Long> values = new TreeMap<>(ByteArrayComparator.getInstance());

    public Memtable() {
	super();
    }

    public void clear() {
	values.clear();
    }

    public void put(byte[] rowKey, long offset) {
	values.put(rowKey, offset);
    }

    public long get(byte[] rowKey) {
	Long offset = values.get(rowKey);
	if (offset == null) {
	    return -1;
	}
	return offset;
    }

    public TreeMap<byte[], Long> getValues() {
	return values;
    }

}
