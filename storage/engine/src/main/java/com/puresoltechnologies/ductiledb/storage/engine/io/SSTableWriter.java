package com.puresoltechnologies.ductiledb.storage.engine.io;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

import com.puresoltechnologies.ductiledb.storage.engine.utils.Bytes;

/**
 * This class is used to write to a commit log provided via OutputStream.
 * 
 * @author Rick-Rainer Ludwig
 */
public class SSTableWriter implements Closeable {

    private final CountingOutputStream outputStream;

    public SSTableWriter(OutputStream outputStream) {
	super();
	this.outputStream = new CountingOutputStream(outputStream);
    }

    public SSTableWriter(OutputStream outputStream, long startOffset) {
	super();
	this.outputStream = new CountingOutputStream(outputStream, startOffset);
    }

    @Override
    public void close() throws IOException {
	outputStream.close();
    }

    public long getOffset() {
	return outputStream.getCount();
    }

    public long write(byte[] rowKey, byte[] key, byte[] value) throws IOException {
	long offset = getOffset();
	outputStream.write(Bytes.toBytes(rowKey.length));
	outputStream.write(rowKey);
	outputStream.write(Bytes.toBytes(key.length));
	outputStream.write(key);
	outputStream.write(Bytes.toBytes(value.length));
	outputStream.write(value);
	return offset;
    }
}
