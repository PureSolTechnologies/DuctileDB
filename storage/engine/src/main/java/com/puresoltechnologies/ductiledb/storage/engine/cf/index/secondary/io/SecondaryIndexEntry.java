package com.puresoltechnologies.ductiledb.storage.engine.cf.index.secondary.io;

import java.util.Arrays;
import java.util.Objects;

import com.puresoltechnologies.ductiledb.storage.engine.cf.index.primary.RowKey;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

/**
 * This class provides the index result.
 * 
 * @author Rick-Rainer Ludwig
 */
public class SecondaryIndexEntry implements Comparable<SecondaryIndexEntry> {

    private final byte[] value;
    private final RowKey rowKey;
    private final int hashCode;

    public SecondaryIndexEntry(byte[] value, RowKey rowKey) {
	super();
	this.value = value;
	this.rowKey = rowKey;
	this.hashCode = Objects.hash(Arrays.hashCode(value), rowKey);
    }

    public byte[] getValue() {
	return value;
    }

    public RowKey getRowKey() {
	return rowKey;
    }

    @Override
    public int compareTo(SecondaryIndexEntry o) {
	return this.rowKey.compareTo(o.rowKey);
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
	SecondaryIndexEntry other = (SecondaryIndexEntry) obj;
	if (hashCode != other.hashCode) {
	    return false;
	}
	if (rowKey == null) {
	    if (other.rowKey != null)
		return false;
	} else if (!rowKey.equals(other.rowKey))
	    return false;
	if (!Arrays.equals(value, other.value))
	    return false;
	return true;
    }

    @Override
    public String toString() {
	return Bytes.toHumanReadableString(value) + " -> " + rowKey;
    }

}
