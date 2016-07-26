package com.puresoltechnologies.ductiledb.storage.engine.io.sstable;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

/**
 * This is a special output stream to increase the buffer size, calculate MD5
 * hash and to count bytes for indizes.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBOutputStream implements Closeable {

    private int bufferPos;
    private long fileOffset;
    private MessageDigest digest = null;

    private final DigestOutputStream stream;
    private final byte[] buffer;
    private final int bufferSize;

    public DuctileDBOutputStream(BufferedOutputStream bufferedOutputStream, int bufferSize) throws StorageException {
	super();
	try {
	    this.stream = new DigestOutputStream(bufferedOutputStream, MessageDigest.getInstance("MD5"));
	    this.buffer = new byte[bufferSize];
	    this.bufferSize = bufferSize;
	    this.bufferPos = 0;
	    this.fileOffset = 0;
	} catch (NoSuchAlgorithmException e) {
	    throw new StorageException("Could not initialize DuctileDBOutputStream.", e);
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

    public long getFileOffset() {
	return fileOffset;
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
	fileOffset += bytes.length;
    }

    public void flush() throws IOException {
	if (bufferPos > 0) {
	    stream.write(buffer, 0, bufferPos);
	    bufferPos = 0;
	}
    }

}
