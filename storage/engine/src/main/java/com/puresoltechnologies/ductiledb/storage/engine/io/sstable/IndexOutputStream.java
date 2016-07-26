package com.puresoltechnologies.ductiledb.storage.engine.io.sstable;

import java.io.BufferedOutputStream;
import java.io.IOException;

import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.io.DuctileDBOutputStream;

public class IndexOutputStream extends DuctileDBOutputStream {

    public IndexOutputStream(BufferedOutputStream bufferedOutputStream, int bufferSize) throws IOException {
	super(bufferedOutputStream, bufferSize);
    }

    public void writeIndexEntry(byte[] rowKey, long offset) throws IOException {
	writeData(Bytes.toBytes(rowKey.length));
	writeData(rowKey);
	writeData(Bytes.toBytes(offset));
    }

}
