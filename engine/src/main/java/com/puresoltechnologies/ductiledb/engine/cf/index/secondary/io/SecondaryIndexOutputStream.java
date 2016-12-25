package com.puresoltechnologies.ductiledb.engine.cf.index.secondary.io;

import java.io.BufferedOutputStream;
import java.io.IOException;

import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.logstore.io.DuctileDBOutputStream;
import com.puresoltechnologies.ductiledb.logstore.utils.Bytes;

public class SecondaryIndexOutputStream extends DuctileDBOutputStream {

    public SecondaryIndexOutputStream(BufferedOutputStream bufferedOutputStream, int bufferSize) throws IOException {
	super(bufferedOutputStream, bufferSize);

    }

    public synchronized void writeIndexEntry(byte[] value, Key rowKey) throws IOException {
	writeData(Bytes.toBytes(value.length));
	writeData(value);
	writeData(Bytes.toBytes(rowKey.getBytes().length));
	writeData(rowKey.getBytes());
    }

}
