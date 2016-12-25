package com.puresoltechnologies.ductiledb.logstore;

import java.util.Arrays;

import com.puresoltechnologies.ductiledb.logstore.utils.ByteArrayComparator;
import com.puresoltechnologies.ductiledb.logstore.utils.Bytes;

public class Key implements Comparable<Key> {

    private static final ByteArrayComparator comparator = ByteArrayComparator.getInstance();

    public static Key fromHexString(String hexString) {
	return new Key(Bytes.fromHexString(hexString));
    }

    public static Key of(byte b) {
	return new Key(Bytes.toBytes(b));
    }

    public static Key of(short s) {
	return new Key(Bytes.toBytes(s));
    }

    public static Key of(int i) {
	return new Key(Bytes.toBytes(i));
    }

    public static Key of(long l) {
	return new Key(Bytes.toBytes(l));
    }

    public static Key of(String s) {
	return new Key(Bytes.toBytes(s));
    }

    public static Key of(byte[] bytes) {
	return new Key(bytes);
    }

    private final int hashCode;
    private final byte[] key;

    protected Key(byte[] key) {
	super();
	if ((key == null) || (key.length == 0)) {
	    throw new IllegalArgumentException("Row keys must not be null or empty.");
	}
	this.key = key;
	int result = 1;
	for (byte element : key) {
	    result = 31 * result + element;
	}
	hashCode = result;
    }

    public byte[] getBytes() {
	return key;
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
	if (!Arrays.equals(key, other.key))
	    return false;
	return true;
    }

    @Override
    public int compareTo(Key o) {
	return comparator.compare(key, o.key);
    }

    @Override
    public String toString() {
	return Bytes.toString(key);
    }

    public String toHexString() {
	return Bytes.toHexString(key);
    }

    public String toHumanReadableString() {
	return Bytes.toHumanReadableString(key);
    }

    public byte toByte() {
	return Bytes.toByte(key);
    }

    public short toShort() {
	return Bytes.toShort(key);
    }

    public int toInt() {
	return Bytes.toInt(key);
    }

    public long toLong() {
	return Bytes.toLong(key);
    }
}
