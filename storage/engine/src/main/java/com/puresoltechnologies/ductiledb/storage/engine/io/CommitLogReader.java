package com.puresoltechnologies.ductiledb.storage.engine.io;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import com.puresoltechnologies.ductiledb.storage.engine.utils.Bytes;

/**
 * This class is used to read a commit log provided via InputStream.
 * 
 * @author Rick-Rainer Ludwig
 */
public class CommitLogReader implements Closeable {

    private final InputStream inputStream;

    public CommitLogReader(InputStream inputStream) {
	super();
	this.inputStream = inputStream;
    }

    @Override
    public void close() throws IOException {
	inputStream.close();
    }

    public CommitLogEntry write() throws IOException {
	byte[] bytes = new byte[4];
	inputStream.read(bytes, 0, 4);
	int length = Bytes.toInt(bytes);
	byte[] rowKey = new byte[length];
	inputStream.read(rowKey, 0, length);
	inputStream.read(bytes, 0, 4);
	length = Bytes.toInt(bytes);
	byte[] key = new byte[length];
	inputStream.read(key, 0, length);
	inputStream.read(bytes, 0, 4);
	length = Bytes.toInt(bytes);
	byte[] value = new byte[length];
	inputStream.read(value, 0, length);
	return new CommitLogEntry(rowKey, key, value);
    }
}
