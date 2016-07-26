package com.puresoltechnologies.ductiledb.storage.engine.io;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;

public class DuctileDBInputStream implements Closeable {

    private final BufferedInputStream stream;

    public DuctileDBInputStream(BufferedInputStream bufferedOutputStream) {
	super();
	this.stream = bufferedOutputStream;
    }

    @Override
    public void close() throws IOException {
	stream.close();
    }

    public long skip(long n) throws IOException {
	return stream.skip(n);
    }

    public int read(byte[] buffer, int off, int len) throws IOException {
	return stream.read(buffer, off, len);
    }

    public int read(byte[] buffer) throws IOException {
	return stream.read(buffer);
    }

}
