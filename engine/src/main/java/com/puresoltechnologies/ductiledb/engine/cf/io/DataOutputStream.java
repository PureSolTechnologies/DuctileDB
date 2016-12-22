package com.puresoltechnologies.ductiledb.engine.cf.io;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Map.Entry;

import com.puresoltechnologies.ductiledb.engine.Key;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnMap;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnValue;
import com.puresoltechnologies.ductiledb.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.engine.io.DuctileDBOutputStream;

import java.util.Set;

public class DataOutputStream extends DuctileDBOutputStream {

    public DataOutputStream(BufferedOutputStream bufferedOutputStream, int bufferSize) throws IOException {
	super(bufferedOutputStream, bufferSize);
    }

    public synchronized void writeRow(Key rowKey, Instant tombstone, ColumnMap columns) throws IOException {
	byte[] key = rowKey.getBytes();
	// Row key
	writeData(Bytes.toBytes(key.length));
	writeData(key);
	// Row tombstone
	writeTombstone(tombstone);
	// Column number
	Set<Entry<Key, ColumnValue>> entrySet = columns.entrySet();
	writeData(Bytes.toBytes(entrySet.size()));
	// Columns
	for (Entry<Key, ColumnValue> column : entrySet) {
	    // Column key
	    Key columnKey = column.getKey();
	    writeData(Bytes.toBytes(columnKey.getBytes().length));
	    writeData(columnKey.getBytes());
	    // Column value
	    ColumnValue columnValue = column.getValue();
	    writeTombstone(columnValue.getTombstone());
	    byte[] value = columnValue.getBytes();
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
