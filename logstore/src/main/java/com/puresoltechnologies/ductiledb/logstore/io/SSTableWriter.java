package com.puresoltechnologies.ductiledb.logstore.io;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.security.MessageDigest;
import java.time.Instant;

import com.puresoltechnologies.ductiledb.commons.Bytes;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.logstore.LogStructuredStore;
import com.puresoltechnologies.ductiledb.logstore.Row;
import com.puresoltechnologies.ductiledb.logstore.data.DataFileWriter;
import com.puresoltechnologies.ductiledb.logstore.index.IndexFileWriter;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
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
    private final DataFileWriter dataFileWriter;
    private final IndexFileWriter indexFileWriter;
    private Key startRowKey = null;
    private long startOffset = -1;
    private Key endRowKey = null;
    private long endOffset = -1;

    public SSTableWriter(Storage storage, File directory, String baseFilename, int bufferSize) {
	super();
	try {
	    this.storage = storage;
	    this.directory = directory;
	    this.baseFilename = baseFilename;
	    this.dataFile = new File(directory, baseFilename + LogStructuredStore.DATA_FILE_SUFFIX);
	    this.indexFile = new File(directory, baseFilename + LogStructuredStore.INDEX_FILE_SUFFIX);
	    this.dataFileWriter = new DataFileWriter(storage, dataFile);
	    this.indexFileWriter = new IndexFileWriter(storage, indexFile, dataFile);
	} catch (IOException e) {
	    throw new StorageException("Could not initialize sstable writer.", e);
	}
    }

    @Override
    public void close() throws IOException {
	dataFileWriter.close();
	indexFileWriter.close();
	writeMD5File();
    }

    private void writeMD5File() throws IOException {
	try (BufferedWriter md5Writer = new BufferedWriter(new OutputStreamWriter(
		storage.create(new File(directory, baseFilename + LogStructuredStore.MD5_FILE_SUFFIX))))) {
	    MessageDigest dataDigest = dataFileWriter.getMessageDigest();
	    md5Writer.write(Bytes.toHexString(dataDigest.digest()) + "  " + dataFile.getName() + "\n");
	    MessageDigest indexDigest = indexFileWriter.getMessageDigest();
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
	return dataFileWriter.getPosition();
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

    public void write(Key rowKey, Instant tombstone, byte[] data) throws IOException {
	if (startRowKey == null) {
	    startRowKey = rowKey;
	    startOffset = dataFileWriter.getPosition();
	}
	endRowKey = rowKey;
	endOffset = dataFileWriter.getPosition();
	dataFileWriter.writeRow(rowKey, tombstone, data);
	indexFileWriter.writeIndexEntry(rowKey, endOffset);
    }

    public void write(Row row) throws IOException {
	write(row.getKey(), row.getTombstone(), row.getData());
    }

}
