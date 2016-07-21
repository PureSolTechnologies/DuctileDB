package com.puresoltechnologies.ductiledb.storage.engine.io;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map.Entry;
import java.util.Set;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.ColumnFamilyEngine;
import com.puresoltechnologies.ductiledb.storage.engine.memtable.ColumnMap;
import com.puresoltechnologies.ductiledb.storage.engine.utils.Bytes;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This class is used to write to a commit log provided via OutputStream.
 * 
 * @author Rick-Rainer Ludwig
 */
public class SSTableWriter implements Closeable {

    private final File dataFile;
    private final File indexFile;

    private final Storage storage;
    private final File directory;
    private final String baseFilename;
    private final DigestOutputStream dataStream;
    private final DigestOutputStream indexStream;
    private final int blockSize;
    private final int bufferSize;
    private long fileOffset;
    private final byte[] dataBuffer;
    private int dataBufferPos;
    private final byte[] indexBuffer;
    private int indexBufferPos;
    private byte[] startRowKey = null;
    private byte[] endRowKey = null;

    public SSTableWriter(Storage storage, File directory, String baseFilename, int blockSize, int bufferSize)
	    throws StorageException {
	super();
	try {
	    this.storage = storage;
	    this.directory = directory;
	    this.baseFilename = baseFilename;
	    this.dataFile = new File(directory, baseFilename + ColumnFamilyEngine.DATA_FILE_SUFFIX);
	    this.indexFile = new File(directory, baseFilename + ColumnFamilyEngine.INDEX_FILE_SUFFIX);
	    this.dataStream = new DigestOutputStream(storage.create(dataFile), MessageDigest.getInstance("MD5"));
	    this.indexStream = new DigestOutputStream(storage.create(indexFile), MessageDigest.getInstance("MD5"));
	    this.blockSize = blockSize;
	    this.bufferSize = bufferSize;
	    this.fileOffset = 0;
	    this.dataBuffer = new byte[this.bufferSize];
	    this.dataBufferPos = 0;
	    this.indexBuffer = new byte[this.bufferSize];
	    this.indexBufferPos = 0;
	} catch (IOException | NoSuchAlgorithmException e) {
	    throw new StorageException("Could not initialize sstable writer.", e);
	}
    }

    @Override
    public void close() throws IOException {
	flushData();
	flushIndex();
	dataStream.close();
	indexStream.close();
	MessageDigest dataDigest = dataStream.getMessageDigest();
	MessageDigest indexDigest = indexStream.getMessageDigest();
	try (BufferedWriter md5Writer = new BufferedWriter(new OutputStreamWriter(new BufferedOutputStream(
		storage.create(new File(directory, baseFilename + ColumnFamilyEngine.MD5_FILE_SUFFIX)), blockSize)))) {
	    md5Writer.write(Bytes.toHexString(dataDigest.digest()) + "  " + dataFile.getName() + "\n");
	    md5Writer.write(Bytes.toHexString(indexDigest.digest()) + "  " + indexFile.getName() + "\n");
	}
    }

    public final File getDataFile() {
	return dataFile;
    }

    public final File getIndexFile() {
	return indexFile;
    }

    public final long getDataFileSize() {
	return fileOffset;
    }

    public final byte[] getStartRowKey() {
	return startRowKey;
    }

    public final byte[] getEndRowKey() {
	return endRowKey;
    }

    public void write(byte[] rowKey, ColumnMap columns) throws IOException {
	if (startRowKey == null) {
	    startRowKey = rowKey;
	}
	endRowKey = rowKey;
	writeData(Bytes.toBytes(rowKey.length));
	writeData(rowKey);
	Set<Entry<byte[], byte[]>> entrySet = columns.entrySet();
	writeData(Bytes.toBytes(entrySet.size()));
	for (Entry<byte[], byte[]> column : entrySet) {
	    byte[] key = column.getKey();
	    byte[] value = column.getValue();
	    writeData(Bytes.toBytes(key.length));
	    writeData(key);
	    writeData(Bytes.toBytes(value.length));
	    writeData(value);
	}
	writeIndex(Bytes.toBytes(rowKey.length));
	writeIndex(rowKey);
	writeIndex(Bytes.toBytes(fileOffset));
    }

    private void writeData(byte[] bytes) throws IOException {
	if (bytes.length > bufferSize - dataBufferPos) {
	    flushData();
	    if (bytes.length > bufferSize) {
		dataStream.write(bytes);
	    } else {
		dataBufferPos += Bytes.putBytes(dataBuffer, bytes, dataBufferPos);
	    }
	} else {
	    dataBufferPos += Bytes.putBytes(dataBuffer, bytes, dataBufferPos);
	}
	fileOffset += bytes.length;
    }

    private void flushData() throws IOException {
	if (dataBufferPos > 0) {
	    dataStream.write(dataBuffer, 0, dataBufferPos);
	    dataBufferPos = 0;
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

    public void write(SSTableDataEntry entry) throws IOException {
	write(entry.getRowKey(), entry.getColumns());
    }

}
