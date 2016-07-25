package com.puresoltechnologies.ductiledb.storage.engine.index;

import java.io.File;

import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.utils.ByteArrayComparator;

/**
 * This class provides the index result.
 * 
 * @author Rick-Rainer Ludwig
 */
public class IndexEntry implements Comparable<IndexEntry> {

    private static final ByteArrayComparator BYTE_ARRAY_COMPARATOR = ByteArrayComparator.getInstance();

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

    @Override
    public int compareTo(IndexEntry o) {
	int cmp = this.dataFile.compareTo(o.dataFile);
	if (cmp != 0) {
	    return cmp;
	}
	return BYTE_ARRAY_COMPARATOR.compare(this.rowKey, o.rowKey);
    }

    @Override
    public String toString() {
	return Bytes.toHumanReadableString(rowKey) + " -> " + dataFile + ":" + offset;
    }
}
