package com.puresoltechnologies.ductiledb.storage.engine;

import java.time.Instant;
import java.util.Arrays;

import com.puresoltechnologies.ductiledb.storage.engine.utils.ArrayComparator;

public class LogEntry implements Comparable<LogEntry> {

    private static final ArrayComparator comparator = new ArrayComparator();

    private final Instant timestamp;
    private final byte[] key;
    private final ColumnEntry[] columns;

    public LogEntry(Instant timestamp, byte[] key, ColumnEntry[] columns) {
	super();
	this.timestamp = timestamp;
	this.key = key;
	this.columns = columns;
	Arrays.sort(this.columns);
    }

    public Instant getTimestamp() {
	return timestamp;
    }

    public byte[] getRowKey() {
	return key;
    }

    public ColumnEntry[] getColumns() {
	return columns;
    }

    @Override
    public int compareTo(LogEntry o) {
	return comparator.compare(key, o.key);
    }

}
