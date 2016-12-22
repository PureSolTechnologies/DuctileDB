package com.puresoltechnologies.ductiledb.engine.cf.index.primary.io;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;

import com.puresoltechnologies.ductiledb.engine.Key;
import com.puresoltechnologies.ductiledb.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.engine.io.DuctileDBOutputStream;

public class IndexOutputStream extends DuctileDBOutputStream {

    public IndexOutputStream(BufferedOutputStream bufferedOutputStream, int bufferSize, File dataFile)
	    throws IOException {
	super(bufferedOutputStream, bufferSize);
	String path = dataFile.getPath();
	writeData(Bytes.toBytes(path.length()));
	writeData(Bytes.toBytes(path));
    }

    public synchronized void writeIndexEntry(Key rowKey, long offset) throws IOException {
	writeData(Bytes.toBytes(rowKey.getBytes().length));
	writeData(rowKey.getBytes());
	writeData(Bytes.toBytes(offset));
    }

}
