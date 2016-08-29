package com.puresoltechnologies.ductiledb.storage.engine.cf;

import java.time.Instant;
import java.util.Arrays;

import com.puresoltechnologies.ductiledb.storage.engine.utils.ByteArrayComparator;

public final class ColumnValue implements Comparable<ColumnValue> {

    private static final ByteArrayComparator COMPARATOR = ByteArrayComparator.getInstance();

    private final byte[] value;
    private final Instant tombstone;

    public ColumnValue(byte[] value, Instant tombstone) {
	super();
	if (value == null) {
	    throw new IllegalArgumentException("Value must not be null.");
	}
	this.value = value;
	this.tombstone = tombstone;
    }

    public byte[] getValue() {
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
	return COMPARATOR.compare(this.getValue(), o.getValue());
    }

}
