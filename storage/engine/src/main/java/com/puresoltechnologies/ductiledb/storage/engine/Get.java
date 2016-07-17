package com.puresoltechnologies.ductiledb.storage.engine;

import java.util.HashSet;
import java.util.Set;

public class Get {

    private final byte[] key;
    private final Set<String> columnFamilies = new HashSet<>();

    public Get(byte[] key) {
	super();
	this.key = key;
    }

    public final byte[] getKey() {
	return key;
    }

    public final void addFamily(String columnFamily) {
	columnFamilies.add(columnFamily);
    }

    public final Set<String> getColumnFamilies() {
	return columnFamilies;
    }

}
