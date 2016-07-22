package com.puresoltechnologies.ductiledb.storage.engine.io.sstable;

/**
 * This class represents a single index file entry.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class SSTableIndexEntry {

    private final byte[] rowKey;
    private final long offset;

    public SSTableIndexEntry(byte[] rowKey, long offset) {
	super();
	this.rowKey = rowKey;
	this.offset = offset;
    }

    public byte[] getRowKey() {
	return rowKey;
    }

    public long getOffset() {
	return offset;
    }

}
