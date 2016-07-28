package com.puresoltechnologies.ductiledb.storage.engine;

import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Put {

    private final Instant timestamp = Instant.now();
    private final Map<byte[], Map<byte[], byte[]>> columns = new HashMap<>();
    private final byte[] key;

    public Put(byte[] key) {
	super();
	this.key = key;
    }

    public final Instant getTimestamp() {
	return timestamp;
    }

    public final byte[] getKey() {
	return key;
    }

    public final Iterator<byte[]> getColumnFamilies() {
	return columns.keySet().iterator();
    }

    public final Map<byte[], byte[]> getColumnValues(byte[] columnFamily) {
	return columns.get(columnFamily);
    }

    public final void addColumn(byte[] columnFamilyName, byte[] key, byte[] value) {
	Map<byte[], byte[]> columnFamily = columns.get(columnFamilyName);
	if (columnFamily == null) {
	    columnFamily = new HashMap<>();
	    columns.put(columnFamilyName, columnFamily);
	}
	columnFamily.put(key, value);
    }

}
