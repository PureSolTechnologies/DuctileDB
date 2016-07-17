package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.File;
import java.time.Instant;

import com.puresoltechnologies.ductiledb.storage.engine.utils.ArrayComparator;

public class IndexEntry implements Comparable<IndexEntry> {

    private static final ArrayComparator comparator = new ArrayComparator();

    private final Instant timestamp;
    private final byte[] key;
    private final File file;
    private final long offset;

    public IndexEntry(Instant timestamp, byte[] key, File file, long offset) {
	super();
	this.timestamp = timestamp;
	this.key = key;
	this.file = file;
	this.offset = offset;
    }

    public Instant getTimestamp() {
	return timestamp;
    }

    public byte[] getKey() {
	return key;
    }

    public File getFile() {
	return file;
    }

    public long getOffset() {
	return offset;
    }

    @Override
    public int compareTo(IndexEntry o) {
	return comparator.compare(key, o.key);
    }

}
