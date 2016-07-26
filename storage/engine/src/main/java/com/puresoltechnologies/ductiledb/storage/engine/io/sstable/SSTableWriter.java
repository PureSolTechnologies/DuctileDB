package com.puresoltechnologies.ductiledb.storage.engine.io.sstable;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.security.MessageDigest;
import java.util.Map.Entry;
import java.util.Set;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.ColumnFamilyEngine;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.memtable.ColumnMap;
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
    private final DuctileDBOutputStream dataStream;
    private final DuctileDBOutputStream indexStream;
    private byte[] startRowKey = null;
    private long startOffset = 0;
    private byte[] endRowKey = null;
    private long endOffset = 0;

    public SSTableWriter(Storage storage, File directory, String baseFilename, int bufferSize) throws StorageException {
	super();
	try {
	    this.storage = storage;
	    this.directory = directory;
	    this.baseFilename = baseFilename;
	    this.dataFile = new File(directory, baseFilename + ColumnFamilyEngine.DATA_FILE_SUFFIX);
	    this.indexFile = new File(directory, baseFilename + ColumnFamilyEngine.INDEX_FILE_SUFFIX);
	    this.dataStream = new DuctileDBOutputStream(storage.create(dataFile), bufferSize);
	    this.indexStream = new DuctileDBOutputStream(storage.create(indexFile), bufferSize);
	} catch (IOException e) {
	    throw new StorageException("Could not initialize sstable writer.", e);
	}
    }

    @Override
    public void close() throws IOException {
	dataStream.close();
	indexStream.close();
	writeMD5File();
    }

    private void writeMD5File() throws IOException {
	try (BufferedWriter md5Writer = new BufferedWriter(new OutputStreamWriter(
		storage.create(new File(directory, baseFilename + ColumnFamilyEngine.MD5_FILE_SUFFIX))))) {
	    MessageDigest dataDigest = dataStream.getMessageDigest();
	    md5Writer.write(Bytes.toHexString(dataDigest.digest()) + "  " + dataFile.getName() + "\n");
	    MessageDigest indexDigest = indexStream.getMessageDigest();
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
	return dataStream.getFileOffset();
    }

    public final byte[] getStartRowKey() {
	return startRowKey;
    }

    public final long getStartOffset() {
	return startOffset;
    }

    public final byte[] getEndRowKey() {
	return endRowKey;
    }

    public final long getEndOffset() {
	return endOffset;
    }

    public void write(byte[] rowKey, ColumnMap columns) throws IOException {
	if (startRowKey == null) {
	    startRowKey = rowKey;
	    startOffset = dataStream.getFileOffset();
	}
	endRowKey = rowKey;
	endOffset = dataStream.getFileOffset();
	dataStream.writeData(Bytes.toBytes(rowKey.length));
	dataStream.writeData(rowKey);
	Set<Entry<byte[], byte[]>> entrySet = columns.entrySet();
	dataStream.writeData(Bytes.toBytes(entrySet.size()));
	for (Entry<byte[], byte[]> column : entrySet) {
	    byte[] key = column.getKey();
	    byte[] value = column.getValue();
	    dataStream.writeData(Bytes.toBytes(key.length));
	    dataStream.writeData(key);
	    dataStream.writeData(Bytes.toBytes(value.length));
	    dataStream.writeData(value);
	}
	indexStream.writeData(Bytes.toBytes(rowKey.length));
	indexStream.writeData(rowKey);
	indexStream.writeData(Bytes.toBytes(endOffset));
    }

    public void write(SSTableDataEntry entry) throws IOException {
	write(entry.getRowKey(), entry.getColumns());
    }

}
