package com.puresoltechnologies.ductiledb.logstore.io;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.time.Instant;

import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.logstore.utils.Bytes;

public class DataOutputStream extends DuctileDBOutputStream {

    public DataOutputStream(BufferedOutputStream bufferedOutputStream, int bufferSize) throws IOException {
	super(bufferedOutputStream, bufferSize);
    }

    public synchronized void writeRow(Key rowKey, Instant tombstone, byte[] data) throws IOException {
	byte[] key = rowKey.getBytes();
	// Row key
	writeData(Bytes.toBytes(key.length));
	writeData(key);
	// Row tombstone
	writeTombstone(tombstone);
	// Column number
	writeData(Bytes.toBytes(data.length));
	writeData(data);
    }

    public void writeTombstone(Instant tombstone) throws IOException {
	if (tombstone == null) {
	    writeData(new byte[12]);
	} else {
	    write(tombstone);
	}
    }
}
