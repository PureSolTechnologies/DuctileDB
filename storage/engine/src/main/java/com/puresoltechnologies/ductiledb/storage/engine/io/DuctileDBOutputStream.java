package com.puresoltechnologies.ductiledb.storage.engine.io;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * This is a special output stream to increase the buffer size, calculate MD5
 * hash and to count bytes for indizes.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBOutputStream implements Closeable {

    private int bufferPos;
    private long offset;
    private MessageDigest digest = null;

    private final DigestOutputStream stream;
    private final byte[] buffer;
    private final int bufferSize;

    public DuctileDBOutputStream(BufferedOutputStream bufferedOutputStream, int bufferSize) throws IOException {
	super();
	try {
	    this.stream = new DigestOutputStream(bufferedOutputStream, MessageDigest.getInstance("MD5"));
	    this.buffer = new byte[bufferSize];
	    this.bufferSize = bufferSize;
	    this.bufferPos = 0;
	    this.offset = 0;
	} catch (NoSuchAlgorithmException e) {
	    throw new IOException("Could not initialize DuctileDBOutputStream.", e);
	}

    }

    @Override
    public void close() throws IOException {
	flush();
	digest = stream.getMessageDigest();

	stream.close();
    }

    public MessageDigest getMessageDigest() {
	return digest;
    }

    public String getMessageDigestString() {
	return Bytes.toHexString(digest.digest());
    }

    public long getOffset() {
	return offset;
    }

    public void writeData(byte[] bytes) throws IOException {
	if (bytes.length > bufferSize - bufferPos) {
	    flush();
	    if (bytes.length > bufferSize) {
		stream.write(bytes);
	    } else {
		bufferPos += Bytes.putBytes(buffer, bytes, bufferPos);
	    }
	} else {
	    bufferPos += Bytes.putBytes(buffer, bytes, bufferPos);
	}
	offset += bytes.length;
    }

    public void flush() throws IOException {
	if (bufferPos > 0) {
	    stream.write(buffer, 0, bufferPos);
	    stream.flush();
	    bufferPos = 0;
	}
    }

}
