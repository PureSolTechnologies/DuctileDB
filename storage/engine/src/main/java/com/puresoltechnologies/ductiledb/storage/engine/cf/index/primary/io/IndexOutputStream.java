package com.puresoltechnologies.ductiledb.storage.engine.cf.index.primary.io;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;

import com.puresoltechnologies.ductiledb.storage.engine.cf.index.primary.RowKey;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.io.DuctileDBOutputStream;

public class IndexOutputStream extends DuctileDBOutputStream {

    public IndexOutputStream(BufferedOutputStream bufferedOutputStream, int bufferSize, File dataFile)
	    throws IOException {
	super(bufferedOutputStream, bufferSize);
	String path = dataFile.getPath();
	writeData(Bytes.toBytes(path.length()));
	writeData(Bytes.toBytes(path));
    }

    public synchronized void writeIndexEntry(RowKey rowKey, long offset) throws IOException {
	writeData(Bytes.toBytes(rowKey.getKey().length));
	writeData(rowKey.getKey());
	writeData(Bytes.toBytes(offset));
    }

}
