package com.puresoltechnologies.ductiledb.logstore.index.io;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;

import com.puresoltechnologies.ductiledb.commons.Bytes;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.logstore.io.DuctileDBOutputStream;

public class IndexOutputStream extends DuctileDBOutputStream {

    public IndexOutputStream(BufferedOutputStream bufferedOutputStream, int bufferSize, File dataFile)
	    throws IOException {
	super(bufferedOutputStream, bufferSize);
	String path = dataFile.getPath();
	writeData(Bytes.fromInt(path.length()));
	writeData(Bytes.fromString(path));
    }

    public synchronized void writeIndexEntry(Key rowKey, long offset) throws IOException {
	writeData(Bytes.fromInt(rowKey.getBytes().length));
	writeData(rowKey.getBytes());
	writeData(Bytes.fromLong(offset));
    }

}
