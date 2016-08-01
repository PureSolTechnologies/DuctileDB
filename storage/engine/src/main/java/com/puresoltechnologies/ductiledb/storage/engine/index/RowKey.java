package com.puresoltechnologies.ductiledb.storage.engine.index;

import java.util.Arrays;

import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.utils.ByteArrayComparator;

public class RowKey implements Comparable<RowKey> {

    private static final ByteArrayComparator comparator = ByteArrayComparator.getInstance();

    private final int hashCode;
    private final byte[] key;

    public RowKey(byte[] rowKey) {
	super();
	this.key = rowKey;
	int result = 1;
	for (byte element : rowKey) {
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
	RowKey other = (RowKey) obj;
	if (hashCode != other.hashCode)
	    return false;
	if (!Arrays.equals(key, other.key))
	    return false;
	return true;
    }

    @Override
    public int compareTo(RowKey o) {
	return comparator.compare(key, o.key);
    }

    @Override
    public String toString() {
	return Bytes.toHumanReadableString(key);
    }
}
