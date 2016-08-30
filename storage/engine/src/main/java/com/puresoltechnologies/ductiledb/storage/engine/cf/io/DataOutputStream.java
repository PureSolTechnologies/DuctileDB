package com.puresoltechnologies.ductiledb.storage.engine.cf.io;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Map.Entry;
import java.util.Set;

import com.puresoltechnologies.ductiledb.storage.engine.Key;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnMap;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnValue;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.io.DuctileDBOutputStream;

public class DataOutputStream extends DuctileDBOutputStream {

    public DataOutputStream(BufferedOutputStream bufferedOutputStream, int bufferSize) throws IOException {
	super(bufferedOutputStream, bufferSize);
    }

    public synchronized void writeRow(Key rowKey, Instant tombstone, ColumnMap columns) throws IOException {
	byte[] key = rowKey.getKey();
	// Row key
	writeData(Bytes.toBytes(key.length));
	writeData(key);
	// Row tombstone
	writeTombstone(tombstone);
	// Column number
	Set<Entry<byte[], ColumnValue>> entrySet = columns.entrySet();
	writeData(Bytes.toBytes(entrySet.size()));
	// Columns
	for (Entry<byte[], ColumnValue> column : entrySet) {
	    // Column key
	    byte[] columnKey = column.getKey();
	    writeData(Bytes.toBytes(columnKey.length));
	    writeData(columnKey);
	    // Column value
	    ColumnValue columnValue = column.getValue();
	    writeTombstone(columnValue.getTombstone());
	    byte[] value = columnValue.getValue();
	    writeData(Bytes.toBytes(value.length));
	    if (value.length > 0) {
		writeData(value);
	    }
	}
    }

    public void writeTombstone(Instant tombstone) throws IOException {
	if (tombstone == null) {
	    writeData(new byte[12]);
	} else {
	    write(tombstone);
	}
    }
}
