package com.puresoltechnologies.ductiledb.storage.engine.memtable;

import java.util.Map;
import java.util.TreeMap;

import com.puresoltechnologies.ductiledb.storage.engine.utils.ByteArrayComparator;

/**
 * Basic Memtable implementation.
 * 
 * @author Rick-Rainer Ludwig
 */
public class MemtableImpl implements Memtable {

    private final ByteArrayComparator byteArrayComparator = new ByteArrayComparator();
    private final Map<byte[], Map<byte[], byte[]>> values = new TreeMap<>(byteArrayComparator);

    public MemtableImpl() {
	super();
    }

    @Override
    public void clear() {
	values.clear();
    }

    @Override
    public void put(byte[] rowKey, byte[] key, byte[] value) {
	Map<byte[], byte[]> row = values.get(rowKey);
	if (row == null) {
	    row = new TreeMap<>(byteArrayComparator);
	    values.put(rowKey, row);
	}
	row.put(key, value);
    }

    @Override
    public Map<byte[], byte[]> get(byte[] rowKey) {
	return values.get(rowKey);
    }

    @Override
    public Map<byte[], Map<byte[], byte[]>> getValues() {
	return values;
    }
}
