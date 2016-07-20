package com.puresoltechnologies.ductiledb.storage.engine.io;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.puresoltechnologies.ductiledb.storage.engine.utils.Bytes;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This class is used to write to a commit log provided via OutputStream.
 * 
 * @author Rick-Rainer Ludwig
 */
public class SSTableWriter implements Closeable {

    private final OutputStream sstableStream;
    private final OutputStream indexStream;
    private final int bufferSize;
    private long fileOffset;
    private final byte[] sstableBuffer;
    private int sstableBufferPos;
    private final byte[] indexBuffer;
    private int indexBufferPos;

    public SSTableWriter(Storage storage, File directory, String baseFilename, int bufferSize) throws IOException {
	this(storage, directory, baseFilename, bufferSize, 0);
    }

    public SSTableWriter(Storage storage, File directory, String baseFilename, int bufferSize, long startOffset)
	    throws IOException {
	super();

	File sstableFile = new File(directory, baseFilename + ".sstable");
	File indexFile = new File(directory, baseFilename + ".index");

	this.sstableStream = storage.create(sstableFile);
	this.indexStream = storage.create(indexFile);
	this.bufferSize = bufferSize;
	this.fileOffset = startOffset;
	this.sstableBuffer = new byte[this.bufferSize];
	this.sstableBufferPos = 0;
	this.indexBuffer = new byte[this.bufferSize];
	this.indexBufferPos = 0;
    }

    @Override
    public void close() throws IOException {
	flushSSTable();
	flushIndex();
	sstableStream.close();
    }

    public void write(byte[] rowKey, Map<byte[], byte[]> columns) throws IOException {
	writeSSTable(Bytes.toBytes(rowKey.length));
	writeSSTable(rowKey);
	Set<Entry<byte[], byte[]>> entrySet = columns.entrySet();
	writeSSTable(Bytes.toBytes(entrySet.size()));
	for (Entry<byte[], byte[]> column : entrySet) {
	    byte[] key = column.getKey();
	    byte[] value = column.getValue();
	    writeSSTable(Bytes.toBytes(key.length));
	    writeSSTable(key);
	    writeSSTable(Bytes.toBytes(value.length));
	    writeSSTable(value);
	}
	writeIndex(Bytes.toBytes(rowKey.length));
	writeIndex(rowKey);
	writeIndex(Bytes.toBytes(fileOffset));
    }

    private void writeSSTable(byte[] bytes) throws IOException {
	if (bytes.length > bufferSize - sstableBufferPos) {
	    flushSSTable();
	    if (bytes.length > bufferSize) {
		sstableStream.write(bytes);
	    } else {
		sstableBufferPos += Bytes.putBytes(sstableBuffer, bytes, sstableBufferPos);
	    }
	} else {
	    sstableBufferPos += Bytes.putBytes(sstableBuffer, bytes, sstableBufferPos);
	}
	fileOffset += bytes.length;
    }

    private void flushSSTable() throws IOException {
	if (sstableBufferPos > 0) {
	    sstableStream.write(sstableBuffer, 0, sstableBufferPos);
	    sstableBufferPos = 0;
	}
    }

    private void writeIndex(byte[] bytes) throws IOException {
	if (bytes.length > bufferSize - indexBufferPos) {
	    flushIndex();
	    if (bytes.length > bufferSize) {
		indexStream.write(bytes);
	    } else {
		indexBufferPos += Bytes.putBytes(indexBuffer, bytes, indexBufferPos);
	    }
	} else {
	    indexBufferPos += Bytes.putBytes(indexBuffer, bytes, indexBufferPos);
	}
    }

    private void flushIndex() throws IOException {
	if (indexBufferPos > 0) {
	    indexStream.write(indexBuffer, 0, indexBufferPos);
	    indexBufferPos = 0;
	}
    }

}
