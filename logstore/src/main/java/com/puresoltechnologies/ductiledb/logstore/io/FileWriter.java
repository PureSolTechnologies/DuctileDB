package com.puresoltechnologies.ductiledb.logstore.io;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;
import java.time.Instant;

import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public abstract class FileWriter<Stream extends DuctileDBOutputStream> implements Closeable {

    private final Storage storage;
    private final File file;
    private Stream stream = null;

    public FileWriter(Storage storage, File file) throws IOException {
	super();
	this.storage = storage;
	this.file = file;
	stream = createStream(storage.open(file));
    }

    protected File getFile() {
	return file;
    }

    protected abstract Stream createStream(BufferedInputStream bufferedInputStream) throws IOException;

    protected Stream getStream() {
	return stream;
    }

    @Override
    public void close() throws IOException {
	stream.close();
    }

    public long getOffset() {
	return stream.getOffset();
    }

    public MessageDigest getMessageDigest() {
	return stream.getMessageDigest();
    }

    public String getMessageDigestString() {
	return stream.getMessageDigestString();
    }

    public void writeData(byte[] bytes) throws IOException {
	stream.writeData(bytes);
    }

    public void write(Instant instant) throws IOException {
	stream.write(instant);
    }

    @Override
    public String toString() {
	return file.toString() + " (position: " + getOffset() + ")";
    }
}
