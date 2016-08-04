package com.puresoltechnologies.ductiledb.storage.engine.io;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;

public class DuctileDBInputStream implements Closeable {

    private long offset;
    private final BufferedInputStream stream;

    public DuctileDBInputStream(BufferedInputStream bufferedOutputStream) {
	super();
	this.stream = bufferedOutputStream;
	this.offset = 0;
    }

    @Override
    public void close() throws IOException {
	stream.close();
    }

    public long getOffset() {
	return offset;
    }

    public long skip(long n) throws IOException {
	long skipped = stream.skip(n);
	offset += skipped;
	return skipped;
    }

    public long goToOffset(long offset) throws IOException {
	if (this.offset > offset) {
	    throw new IOException("The offset was already passed.");
	}
	if (this.offset < offset) {
	    return skip(offset - this.offset);
	}
	return 0;
    }

    public int read(byte[] buffer, int off, int len) throws IOException {
	int bytesRead = stream.read(buffer, off, len);
	offset += bytesRead;
	return bytesRead;
    }

    public int read(byte[] buffer) throws IOException {
	int bytesRead = stream.read(buffer);
	offset += bytesRead;
	return bytesRead;
    }

    public boolean isEof() throws IOException {
	return stream.available() <= 0;
    }

}
