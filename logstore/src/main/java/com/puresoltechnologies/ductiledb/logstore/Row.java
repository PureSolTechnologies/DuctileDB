package com.puresoltechnologies.ductiledb.logstore;

import java.time.Instant;

/**
 * This object is used to contain a generic row.
 * 
 * @author Rick-Rainer Ludwig
 *
 * @param <Value>
 */
public class Row {

    private final Key key;
    private final Instant tombstone;
    private final byte[] data;

    public Row(Key key, Instant tombstone, byte[] value) {
	super();
	this.key = key;
	this.tombstone = tombstone;
	this.data = value;
    }

    public Key getKey() {
	return key;
    }

    public Instant getTombstone() {
	return tombstone;
    }

    public byte[] getData() {
	return data;
    }

    public boolean wasDeleted() {
	return tombstone != null;
    }

}
