package com.puresoltechnologies.ductiledb.storage.engine.io;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

public class BinaryFileInputStream extends BufferedInputStream {

    private final byte[] intBuffer = new byte[4];
    private final byte[] longBuffer = new byte[4];

    public BinaryFileInputStream(InputStream in, int size) {
	super(in, size);
    }

    public BinaryFileInputStream(InputStream in) {
	super(in);
    }

    public int readInt() throws IOException {
	int len = read(intBuffer);
	if (len != 4) {
	    throw new IOException("Could not read full number of bytes needed. It is maybe a broken stream or file.");
	}
	return Bytes.toInt(intBuffer);
    }

    public long readLong() throws IOException {
	int len = read(longBuffer);
	if (len != 8) {
	    throw new IOException("Could not read full number of bytes needed. It is maybe a broken stream or file.");
	}
	return Bytes.toLong(longBuffer);
    }

    public String readString() throws IOException {
	int len = read(intBuffer);
	if (len != 4) {
	    throw new IOException("Could not read full number of bytes needed. It is maybe a broken stream or file.");
	}
	int stringLength = Bytes.toInt(intBuffer);
	byte[] stringBuffer = new byte[stringLength];
	return Bytes.toString(stringBuffer);
    }

}
