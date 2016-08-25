package com.puresoltechnologies.ductiledb.storage.engine.io.index.secondary;

import java.io.BufferedOutputStream;
import java.io.IOException;

import com.puresoltechnologies.ductiledb.storage.engine.index.RowKey;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.io.DuctileDBOutputStream;

public class SecondaryIndexOutputStream extends DuctileDBOutputStream {

    public SecondaryIndexOutputStream(BufferedOutputStream bufferedOutputStream, int bufferSize) throws IOException {
	super(bufferedOutputStream, bufferSize);

    }

    public synchronized void writeIndexEntry(byte[] value, RowKey rowKey) throws IOException {
	writeData(Bytes.toBytes(value.length));
	writeData(value);
	writeData(Bytes.toBytes(rowKey.getKey().length));
	writeData(rowKey.getKey());
    }

}
