package com.puresoltechnologies.ductiledb.storage.engine;

public class Put {

    private final byte[] key;

    public Put(byte[] key) {
	super();
	this.key = key;
    }

    public void addColumn(String columnFamilyName, byte[] key, byte[] value) {
	// TODO Auto-generated method stub

    }

    public void addColumn(byte[] nameBytes, byte[] bytes, byte[] empty) {
	// TODO Auto-generated method stub

    }

}
