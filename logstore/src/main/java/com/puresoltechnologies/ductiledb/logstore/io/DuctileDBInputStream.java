package com.puresoltechnologies.ductiledb.logstore.io;

import java.io.IOException;
import java.io.InputStream;

import com.puresoltechnologies.ductiledb.storage.spi.StorageInputStream;

public class DuctileDBInputStream extends InputStream {

    private final StorageInputStream storageInputStream;

    public DuctileDBInputStream(StorageInputStream bufferedOutputStream) {
	super();
	this.storageInputStream = bufferedOutputStream;
    }

    @Override
    public int read() throws IOException {
	return storageInputStream.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
	return storageInputStream.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
	return storageInputStream.read(b, off, len);
    }

    public final long getPosition() {
	return storageInputStream.getPosition();
    }

    public final long seek(long position) throws IOException {
	return storageInputStream.seek(position);
    }

    @Override
    public long skip(long n) throws IOException {
	return storageInputStream.skip(n);
    }

    @Override
    public int available() throws IOException {
	return storageInputStream.available();
    }

    @Override
    public void close() throws IOException {
	storageInputStream.close();
    }

}
