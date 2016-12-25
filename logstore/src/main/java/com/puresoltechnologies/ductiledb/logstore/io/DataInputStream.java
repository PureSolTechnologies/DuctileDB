package com.puresoltechnologies.ductiledb.logstore.io;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.time.Instant;

import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.logstore.Row;
import com.puresoltechnologies.ductiledb.logstore.utils.Bytes;

public class DataInputStream extends DuctileDBInputStream {

    public DataInputStream(BufferedInputStream bufferedOutputStream) {
	super(bufferedOutputStream);
    }

    public Row readRow() throws IOException {
	byte[] buffer = new byte[12];
	// Read row key
	int len = read(buffer, 0, 4);
	if (len == -1) {
	    return null;
	} else if (len < 4) {
	    throw new IOException("Could not read full number of bytes needed. It is maybe a broken data file.");
	}
	int length = Bytes.toInt(buffer);
	byte[] rowKeyBytes = new byte[length];
	len = read(rowKeyBytes);
	if (len < length) {
	    throw new IOException("Could not read full number of bytes needed. It is maybe a broken data file.");
	}
	Key rowKey = Key.of(rowKeyBytes);
	// Read tombstone
	len = read(buffer, 0, 12);
	if (len < 12) {
	    throw new IOException("Could not read full number of bytes needed. It is maybe a broken data file.");
	}
	Instant tombstone = Bytes.toTombstone(buffer);
	// Read value
	len = read(buffer, 0, 4);
	if (len == -1) {
	    return null;
	} else if (len < 4) {
	    throw new IOException("Could not read full number of bytes needed. It is maybe a broken data file.");
	}
	length = Bytes.toInt(buffer);
	byte[] rowDataBytes = new byte[length];
	len = read(rowDataBytes);
	if (len < length) {
	    throw new IOException("Could not read full number of bytes needed. It is maybe a broken data file.");
	}
	return new Row(rowKey, tombstone, rowDataBytes);
    }

}
