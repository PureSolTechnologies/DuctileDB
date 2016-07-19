package com.puresoltechnologies.ductiledb.storage.engine.io;

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

    private final OutputStream sstableStream;
    private final int bufferSize;
    private long fileOffset;
    private final byte[] buffer;
    private int bufPos;

    public SSTableWriter(OutputStream sstableStream, int bufferSize) {
	this(sstableStream, bufferSize, 0);
    }

    public SSTableWriter(OutputStream sstableStream, int bufferSize, long startOffset) {
	super();
	this.sstableStream = sstableStream;
	this.bufferSize = bufferSize;
	this.fileOffset = startOffset;
	this.buffer = new byte[this.bufferSize];
	this.bufPos = 0;
    }

    @Override
    public void close() throws IOException {
	flush();
	sstableStream.close();
    }

    public long getOffset() {
	return fileOffset;
    }

    public long write(byte[] rowKey, Map<byte[], byte[]> columns) throws IOException {
	long fileOffset = getOffset();
	write(Bytes.toBytes(rowKey.length));
	write(rowKey);
	for (Entry<byte[], byte[]> column : columns.entrySet()) {
	    byte[] key = column.getKey();
	    byte[] value = column.getValue();
	    write(Bytes.toBytes(key.length));
	    write(key);
	    write(Bytes.toBytes(value.length));
	    write(value);
	}
	return fileOffset;
    }

    private void write(byte[] bytes) throws IOException {
	if (bytes.length > bufferSize - bufPos) {
	    flush();
	    if (bytes.length > bufferSize) {
		sstableStream.write(bytes);
	    }
	} else {
	    bufPos += Bytes.putBytes(buffer, bytes, bufPos);
	}
	fileOffset += bytes.length;
    }

    private void flush() throws IOException {
	if (bufPos > 0) {
	    sstableStream.write(buffer, 0, bufPos);
	    bufPos = 0;
	}
    }

}
