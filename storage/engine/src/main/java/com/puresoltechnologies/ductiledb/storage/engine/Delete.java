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

    public void addColumns(byte[] columnFamily, byte[] bytes) {
	// TODO Auto-generated method stub

    }

    public void addFamily(byte[] columnFamily) {
	// TODO Auto-generated method stub

    }

    public Set<byte[]> getColumnFamilies() {
	// TODO Auto-generated method stub
	return null;
    }

}
