package com.puresoltechnologies.ductiledb.storage.engine.io.index;

import java.io.BufferedOutputStream;
import java.io.IOException;

import com.puresoltechnologies.ductiledb.storage.engine.index.RowKey;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.io.DuctileDBOutputStream;

public class IndexOutputStream extends DuctileDBOutputStream {

    public IndexOutputStream(BufferedOutputStream bufferedOutputStream, int bufferSize) throws IOException {
	super(bufferedOutputStream, bufferSize);
    }

    public void writeIndexEntry(RowKey rowKey, long offset) throws IOException {
	writeData(Bytes.toBytes(rowKey.getKey().length));
	writeData(rowKey.getKey());
	writeData(Bytes.toBytes(offset));
    }

}
