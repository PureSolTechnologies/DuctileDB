package com.puresoltechnologies.ductiledb.storage.engine.cf.io;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.security.MessageDigest;
import java.time.Instant;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.Key;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyEngine;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyRow;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnMap;
import com.puresoltechnologies.ductiledb.storage.engine.cf.index.primary.io.IndexOutputStream;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
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
    private final DataOutputStream dataStream;
    private final IndexOutputStream indexStream;
    private Key startRowKey = null;
    private long startOffset = -1;
    private Key endRowKey = null;
    private long endOffset = -1;

    public SSTableWriter(Storage storage, File directory, String baseFilename, int bufferSize) throws StorageException {
	super();
	try {
	    this.storage = storage;
	    this.directory = directory;
	    this.baseFilename = baseFilename;
	    this.dataFile = new File(directory, baseFilename + ColumnFamilyEngine.DATA_FILE_SUFFIX);
	    this.indexFile = new File(directory, baseFilename + ColumnFamilyEngine.INDEX_FILE_SUFFIX);
	    this.dataStream = new DataOutputStream(storage.create(dataFile), bufferSize);
	    this.indexStream = new IndexOutputStream(storage.create(indexFile), bufferSize, dataFile);
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
	return dataStream.getOffset();
    }

    public final Key getStartRowKey() {
	return startRowKey;
    }

    public final long getStartOffset() {
	return startOffset;
    }

    public final Key getEndRowKey() {
	return endRowKey;
    }

    public final long getEndOffset() {
	return endOffset;
    }

    public final boolean hasIndexInformation() {
	return (startOffset >= 0) && (endOffset >= 0) && (startRowKey != null) && (endRowKey != null);
    }

    public void write(Key rowKey, Instant tombstone, ColumnMap columns) throws IOException {
	if (startRowKey == null) {
	    startRowKey = rowKey;
	    startOffset = dataStream.getOffset();
	}
	endRowKey = rowKey;
	endOffset = dataStream.getOffset();
	dataStream.writeRow(rowKey, tombstone, columns);
	indexStream.writeIndexEntry(rowKey, endOffset);
    }

    public void write(ColumnFamilyRow entry) throws IOException {
	write(entry.getRowKey(), entry.getTombstone(), entry.getColumnMap());
    }

}
