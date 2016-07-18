package com.puresoltechnologies.ductiledb.storage.engine.io;

public class CommitLogEntry {

    private final byte[] rowKey;
    private final byte[] key;
    private final byte[] value;

    public CommitLogEntry(byte[] rowKey, byte[] key, byte[] value) {
	super();
	this.rowKey = rowKey;
	this.key = key;
	this.value = value;
    }

    public final byte[] getRowKey() {
	return rowKey;
    }

    public final byte[] getKey() {
	return key;
    }

    public final byte[] getValue() {
	return value;
    }

}
