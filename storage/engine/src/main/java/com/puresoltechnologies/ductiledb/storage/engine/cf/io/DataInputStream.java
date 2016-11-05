package com.puresoltechnologies.ductiledb.storage.engine.cf.io;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.time.Instant;

import com.puresoltechnologies.ductiledb.storage.engine.Key;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyRow;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnMap;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnValue;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.io.DuctileDBInputStream;

public class DataInputStream extends DuctileDBInputStream {

    public DataInputStream(BufferedInputStream bufferedOutputStream) {
	super(bufferedOutputStream);
    }

    public ColumnFamilyRow readRow() throws IOException {
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
	// Read column count
	len = read(buffer, 0, 4);
	if (len < 4) {
	    throw new IOException("Could not read full number of bytes needed. It is maybe a broken data file.");
	}
	int columnCount = Bytes.toInt(buffer);
	// Read columns
	ColumnMap columns = new ColumnMap();
	for (int i = 0; i < columnCount; ++i) {
	    // Read column key...
	    len = read(buffer, 0, 4);
	    if (len < 4) {
		throw new IOException("Could not read full number of bytes needed. It is maybe a broken data file.");
	    }
	    length = Bytes.toInt(buffer);
	    byte[] columnKey = new byte[length];
	    len = read(columnKey);
	    if (len < length) {
		throw new IOException("Could not read full number of bytes needed. It is maybe a broken data file.");
	    }
	    // Read column tombstone...
	    len = read(buffer, 0, 12);
	    if (len < 12) {
		throw new IOException("Could not read full number of bytes needed. It is maybe a broken data file.");
	    }
	    Instant columnTombstone = Bytes.toTombstone(buffer);
	    // Read column value...
	    len = read(buffer, 0, 4);
	    if (len < 4) {
		throw new IOException("Could not read full number of bytes needed. It is maybe a broken data file.");
	    }
	    length = Bytes.toInt(buffer);
	    byte[] columnValue = new byte[length];
	    if (length > 0) {
		len = read(columnValue);
		if (len < length) {
		    throw new IOException(
			    "Could not read full number of bytes needed. It is maybe a broken data file.");
		}
	    }
	    columns.put(Key.of(columnKey), ColumnValue.of(columnValue, columnTombstone));
	}
	return new ColumnFamilyRow(rowKey, columns, tombstone);
    }

}
