package com.puresoltechnologies.ductiledb.storage.engine;

public class Delete {

    private final byte[] key;

    public Delete(byte[] key) {
	super();
	this.key = key;
    }

    public byte[] getKey() {
	return key;
    }

    public void addColumns(byte[] nameBytes, byte[] bytes) {
	// TODO Auto-generated method stub

    }

    public void addFamily(byte[] nameBytes) {
	// TODO Auto-generated method stub

    }

}
