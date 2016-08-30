package com.puresoltechnologies.ductiledb.storage.engine;

import java.util.Arrays;

import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.utils.ByteArrayComparator;

public class Key implements Comparable<Key> {

    private static final ByteArrayComparator comparator = ByteArrayComparator.getInstance();

    private final int hashCode;
    private final byte[] key;

    public Key(byte[] key) {
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

    public byte[] getKey() {
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
	return Bytes.toHumanReadableString(key);
    }
}
