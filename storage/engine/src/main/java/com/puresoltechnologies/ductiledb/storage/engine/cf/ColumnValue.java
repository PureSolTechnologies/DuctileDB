package com.puresoltechnologies.ductiledb.storage.engine.cf;

import java.time.Instant;
import java.util.Arrays;

import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.utils.ByteArrayComparator;

public final class ColumnValue implements Comparable<ColumnValue> {

    private static final ByteArrayComparator COMPARATOR = ByteArrayComparator.getInstance();

    public static ColumnValue of(byte b) {
	return new ColumnValue(Bytes.toBytes(b));
    }

    public static ColumnValue of(short s) {
	return new ColumnValue(Bytes.toBytes(s));
    }

    public static ColumnValue of(int i) {
	return new ColumnValue(Bytes.toBytes(i));
    }

    public static ColumnValue of(long l) {
	return new ColumnValue(Bytes.toBytes(l));
    }

    public static ColumnValue of(String s) {
	return new ColumnValue(Bytes.toBytes(s));
    }

    public static ColumnValue of(byte[] keyBytes) {
	return new ColumnValue(keyBytes);
    }

    public static ColumnValue of(byte b, Instant timestamp) {
	return new ColumnValue(Bytes.toBytes(b), timestamp);
    }

    public static ColumnValue of(short s, Instant timestamp) {
	return new ColumnValue(Bytes.toBytes(s), timestamp);
    }

    public static ColumnValue of(int i, Instant timestamp) {
	return new ColumnValue(Bytes.toBytes(i), timestamp);
    }

    public static ColumnValue of(long l, Instant timestamp) {
	return new ColumnValue(Bytes.toBytes(l), timestamp);
    }

    public static ColumnValue of(String s, Instant timestamp) {
	return new ColumnValue(Bytes.toBytes(s), timestamp);
    }

    public static ColumnValue of(byte[] columnValue, Instant timestamp) {
	return new ColumnValue(columnValue, timestamp);
    }

    public static ColumnValue empty() {
	return new ColumnValue(Bytes.empty());
    }

    private final byte[] value;
    private final Instant tombstone;

    private ColumnValue(byte[] value) {
	this(value, null);
    }

    private ColumnValue(byte[] value, Instant tombstone) {
	super();
	if (value == null) {
	    throw new IllegalArgumentException("Value must not be null.");
	}
	this.value = value;
	this.tombstone = tombstone;
    }

    public byte[] getBytes() {
	return value;
    }

    public Instant getTombstone() {
	return tombstone;
    }

    public boolean wasDeleted() {
	return tombstone != null;
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((tombstone == null) ? 0 : tombstone.hashCode());
	result = prime * result + Arrays.hashCode(value);
	return result;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (getClass() != obj.getClass())
	    return false;
	ColumnValue other = (ColumnValue) obj;
	if (tombstone == null) {
	    if (other.tombstone != null)
		return false;
	} else if (!tombstone.equals(other.tombstone))
	    return false;
	if (!Arrays.equals(value, other.value))
	    return false;
	return true;
    }

    @Override
    public int compareTo(ColumnValue o) {
	return COMPARATOR.compare(this.getBytes(), o.getBytes());
    }

    public byte toByte() {
	return Bytes.toByte(value);
    }

    public short toShort() {
	return Bytes.toShort(value);
    }

    public int toInt() {
	return Bytes.toInt(value);
    }

    public long toLong() {
	return Bytes.toLong(value);
    }

    @Override
    public String toString() {
	return Bytes.toString(value);
    }

}
