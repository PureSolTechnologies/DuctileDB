package com.puresoltechnologies.ductiledb.storage.engine;

import java.util.Set;

public class Delete {

    private final byte[] key;

    public Delete(byte[] key) {
	super();
	this.key = key;
    }

    public final byte[] getKey() {
	return key;
    }

    public void addColumns(byte[] nameBytes, byte[] bytes) {
	// TODO Auto-generated method stub

    }

    public void addFamily(String columnFamily) {
	// TODO Auto-generated method stub

    }

    public Set<String> getColumnFamilies() {
	// TODO Auto-generated method stub
	return null;
    }

}
