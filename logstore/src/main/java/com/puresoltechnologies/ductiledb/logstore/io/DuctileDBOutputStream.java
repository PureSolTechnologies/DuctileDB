package com.puresoltechnologies.ductiledb.logstore.io;

import java.io.IOException;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;

import com.puresoltechnologies.ductiledb.commons.Bytes;
import com.puresoltechnologies.ductiledb.storage.spi.StorageOutputStream;

/**
 * This is a special output stream to increase the buffer size, calculate MD5
 * hash and to count bytes for indizes.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBOutputStream extends OutputStream {

    private MessageDigest digest = null;

    private final StorageOutputStream storageOutputStream;
    private final DigestOutputStream stream;

    public DuctileDBOutputStream(StorageOutputStream storageOutputStream) throws IOException {
	super();
	this.storageOutputStream = storageOutputStream;
	try {
	    this.stream = new DigestOutputStream(storageOutputStream, MessageDigest.getInstance("MD5"));
	} catch (NoSuchAlgorithmException e) {
	    throw new IOException("Could not initialize DuctileDBOutputStream.", e);
	}
    }

    @Override
    public void write(byte[] b) throws IOException {
	stream.write(b);
    }

    @Override
    public void write(int b) throws IOException {
	stream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
	stream.write(b, off, len);
    }

    @Override
    public void close() throws IOException {
	stream.close();
    }

    public MessageDigest getMessageDigest() {
	return digest;
    }

    public String getMessageDigestString() {
	return digest != null ? Bytes.toHexString(digest.digest()) : null;
    }

    public synchronized void writeData(byte[] bytes) throws IOException {
	stream.write(bytes);
    }

    public void write(Instant instant) throws IOException {
	writeData(Bytes.fromInstant(instant));
    }

    @Override
    public synchronized void flush() throws IOException {
	stream.flush();
    }

    public long getPosition() {
	return storageOutputStream.getPosition();
    }

}
