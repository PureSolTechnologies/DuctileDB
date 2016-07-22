package com.puresoltechnologies.ductiledb.storage.engine.io.commitlog;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

import com.puresoltechnologies.ductiledb.storage.engine.utils.Bytes;

/**
 * This class is used to write to a commit log provided via OutputStream.
 * 
 * @author Rick-Rainer Ludwig
 */
public class CommitLogWriter implements Closeable {

    private final OutputStream outputStream;

    public CommitLogWriter(OutputStream outputStream) {
	super();
	this.outputStream = outputStream;
    }

    @Override
    public void close() throws IOException {
	outputStream.close();
    }

    public void write(byte[] rowKey, byte[] key, byte[] value) throws IOException {
	outputStream.write(Bytes.toBytes(rowKey.length));
	outputStream.write(rowKey);
	outputStream.write(Bytes.toBytes(key.length));
	outputStream.write(key);
	outputStream.write(Bytes.toBytes(value.length));
	outputStream.write(value);
    }
}
