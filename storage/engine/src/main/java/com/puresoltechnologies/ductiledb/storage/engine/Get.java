package com.puresoltechnologies.ductiledb.storage.engine;

import java.util.HashSet;
import java.util.Set;

public class Get {

    private final byte[] key;
    private final Set<byte[]> columnFamilies = new HashSet<>();

    public Get(byte[] key) {
	super();
	this.key = key;
    }

    public final byte[] getKey() {
	return key;
    }

    public final void addFamily(byte[] columnFamily) {
	columnFamilies.add(columnFamily);
    }

    public final Set<byte[]> getColumnFamilies() {
	return columnFamilies;
    }

}
