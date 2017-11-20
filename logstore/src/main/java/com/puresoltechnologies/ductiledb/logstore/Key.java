package com.puresoltechnologies.ductiledb.logstore;

import java.util.Arrays;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.puresoltechnologies.ductiledb.commons.Bytes;
import com.puresoltechnologies.ductiledb.logstore.utils.ByteArrayComparator;

public class Key implements Comparable<Key> {

    private static final ByteArrayComparator comparator = ByteArrayComparator.getInstance();

    public static Key fromHexString(String hexString) {
	return new Key(Bytes.fromHexString(hexString));
    }

    public static Key of(byte b) {
	return new Key(Bytes.fromByte(b));
    }

    public static Key of(short s) {
	return new Key(Bytes.fromShort(s));
    }

    public static Key of(int i) {
	return new Key(Bytes.fromInt(i));
    }

    public static Key of(long l) {
	return new Key(Bytes.fromLong(l));
    }

    public static Key of(String s) {
	return new Key(Bytes.fromString(s));
    }

    @JsonCreator
    public static Key of(@JsonProperty("bytes") byte[] bytes) {
	return new Key(bytes);
    }

    private final int hashCode;
    private final byte[] bytes;

    protected Key(byte[] bytes) {
	super();
	if ((bytes == null) || (bytes.length == 0)) {
	    throw new IllegalArgumentException("Row keys must not be null or empty.");
	}
	this.bytes = bytes;
	int result = 1;
	for (byte element : bytes) {
	    result = 31 * result + element;
	}
	hashCode = result;
    }

    public byte[] getBytes() {
	return bytes;
    }

    @Override
    public int hashCode() {
	return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (getClass() != obj.getClass())
	    return false;
	Key other = (Key) obj;
	if (hashCode != other.hashCode)
	    return false;
	if (!Arrays.equals(bytes, other.bytes))
	    return false;
	return true;
    }

    @Override
    public int compareTo(Key o) {
	return comparator.compare(bytes, o.bytes);
    }

    @Override
    public String toString() {
	return Bytes.toHumanReadableString(bytes);
    }

    public String toHexString() {
	return Bytes.toHexString(bytes);
    }

    public String toStringValue() {
	return Bytes.toString(bytes);
    }

    public String toHumanReadableString() {
	return Bytes.toHumanReadableString(bytes);
    }

    public byte toByteValue() {
	return Bytes.toByte(bytes);
    }

    public short toShortValue() {
	return Bytes.toShort(bytes);
    }

    public int toIntValue() {
	return Bytes.toInt(bytes);
    }

    public long toLongValue() {
	return Bytes.toLong(bytes);
    }
}
