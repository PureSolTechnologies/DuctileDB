package com.puresoltechnologies.ductiledb.storage.engine.index;

import java.io.File;

/**
 * This class provides the index result.
 * 
 * @author Rick-Rainer Ludwig
 */
public class IndexEntry {

    private final byte[] rowKey;
    private final File dataFile;
    private final long offset;

    public IndexEntry(byte[] rowKey, File dataFile, long offset) {
	super();
	this.rowKey = rowKey;
	this.dataFile = dataFile;
	this.offset = offset;
    }

    public byte[] getRowKey() {
	return rowKey;
    }

    public File getDataFile() {
	return dataFile;
    }

    public long getOffset() {
	return offset;
    }

}
