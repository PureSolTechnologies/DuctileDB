package com.puresoltechnologies.ductiledb.storage.engine.io;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Map.Entry;

import com.puresoltechnologies.ductiledb.storage.engine.utils.Bytes;

/**
 * This class is used to write to a commit log provided via OutputStream.
 * 
 * @author Rick-Rainer Ludwig
 */
public class SSTableWriter implements Closeable {

    private final CountingOutputStream outputStream;

    public SSTableWriter(OutputStream outputStream, int blockSize) {
	super();
	BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream, blockSize);
	this.outputStream = new CountingOutputStream(bufferedOutputStream);
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

    public long write(byte[] rowKey, Map<byte[], byte[]> columns) throws IOException {
	long offset = getOffset();
	int size = rowKey.length + 4;
	for (Entry<byte[], byte[]> column : columns.entrySet()) {
	    byte[] key = column.getKey();
	    byte[] value = column.getValue();
	    size += key.length + 4;
	    size += value.length + 4;
	}
	byte[] bytes = new byte[size];
	int pos = Bytes.putBytes(bytes, rowKey.length, 0);
	pos += Bytes.putBytes(bytes, rowKey, pos);
	for (Entry<byte[], byte[]> column : columns.entrySet()) {
	    byte[] key = column.getKey();
	    byte[] value = column.getValue();
	    pos += Bytes.putBytes(bytes, key.length, pos);
	    pos += Bytes.putBytes(bytes, key, pos);
	    pos += Bytes.putBytes(bytes, value.length, pos);
	    pos += Bytes.putBytes(bytes, value, pos);
	}
	outputStream.write(bytes, 0, pos);
	return offset;
    }
}
