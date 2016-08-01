package com.puresoltechnologies.ductiledb.storage.engine.index;

import java.io.File;

/**
 * This class provides the index result.
 * 
 * @author Rick-Rainer Ludwig
 */
public class IndexEntry implements Comparable<IndexEntry> {

    private final RowKey rowKey;
    private final File dataFile;
    private final long offset;

    public IndexEntry(RowKey rowKey, File dataFile, long offset) {
	super();
	this.rowKey = rowKey;
	this.dataFile = dataFile;
	this.offset = offset;
    }

    public RowKey getRowKey() {
	return rowKey;
    }

    public File getDataFile() {
	return dataFile;
    }

    public long getOffset() {
	return offset;
    }

    @Override
    public int compareTo(IndexEntry o) {
	int cmp = this.dataFile.compareTo(o.dataFile);
	if (cmp != 0) {
	    return cmp;
	}
	return this.rowKey.compareTo(o.rowKey);
    }

    @Override
    public String toString() {
	return rowKey + " -> " + dataFile + ":" + offset;
    }

    public boolean wasDeleted() {
	return offset < 0;
    }
}
