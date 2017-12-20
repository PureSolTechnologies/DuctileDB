package com.puresoltechnologies.ductiledb.logstore.index;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.commons.misc.io.CloseableIterable;
import com.puresoltechnologies.ductiledb.commons.Bytes;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.logstore.io.DuctileDBInputStream;
import com.puresoltechnologies.ductiledb.logstore.io.FileReader;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class IndexFileReader extends FileReader<DuctileDBInputStream> implements CloseableIterable<IndexEntry> {

    private static final Logger logger = LoggerFactory.getLogger(IndexFileReader.class);

    private static String readDataFile(InputStream inputStream) throws IOException {
	byte[] buffer = new byte[4];
	int len = inputStream.read(buffer, 0, 4);
	if (len < 4) {
	    throw new IOException("Could not read full number of bytes needed. It is maybe a broken index file.");
	}
	int fileNameLength = Bytes.toInt(buffer);
	byte[] fileName = new byte[fileNameLength];
	len = inputStream.read(fileName);
	if (len < fileNameLength) {
	    throw new IOException("Could not read full number of bytes needed. It is maybe a broken index file.");
	}
	return Bytes.toString(fileName);
    }

    private static IndexEntry readEntry(File dataFile, InputStream inputStream) throws IOException {
	byte[] buffer = new byte[8];
	int len = inputStream.read(buffer, 0, 4);
	if (len == -1) {
	    return null;
	} else if (len < 4) {
	    throw new IOException("Could not read full number of bytes needed. It is maybe a broken index file.");
	}
	int keyLength = Bytes.toInt(buffer);
	byte[] rowKey = new byte[keyLength];
	len = inputStream.read(rowKey);
	if (len < keyLength) {
	    throw new IOException("Could not read full number of bytes needed. It is maybe a broken index file.");
	}
	len = inputStream.read(buffer, 0, 8);
	if (len < 8) {
	    throw new IOException("Could not read full number of bytes needed. It is maybe a broken index file.");
	}
	long offset = Bytes.toLong(buffer);
	return new IndexEntry(Key.of(rowKey), dataFile, offset);
    }

    private final File dataFile;

    public IndexFileReader(Storage storage, File indexFile) throws IOException {
	super(storage, indexFile);
	this.dataFile = new File(readDataFile(getStream()));
    }

    @Override
    protected DuctileDBInputStream createStream(BufferedInputStream bufferedInputStream) throws IOException {
	return new DuctileDBInputStream(bufferedInputStream);
    }

    public IndexEntry get() throws IOException {
	return readEntry(dataFile, getStream());
    }

    public IndexEntry get(long offset) throws IOException {
	seek(offset);
	return readEntry(dataFile, getStream());
    }

    public IndexEntry get(Key rowKey) throws IOException {
	while (!getStream().isEof()) {
	    IndexEntry indexEntry = readEntry(dataFile, getStream());
	    int compareResult = indexEntry.getRowKey().compareTo(rowKey);
	    if (compareResult == 0) {
		return indexEntry;
	    } else if (compareResult > 0) {
		return null;
	    }
	}
	return null;
    }

    @Override
    public IndexEntryIterator iterator() {
	return new IndexEntryIterator() {

	    @Override
	    public Key getStartRowKey() {
		return null;
	    }

	    @Override
	    public Key getEndRowKey() {
		return null;
	    }

	    @Override
	    protected IndexEntry findNext() {
		try {
		    return readEntry(dataFile, getStream());
		} catch (IOException e) {
		    logger.error("Could not read next index entry.", e);
		    return null;
		}
	    }

	};
    }

    public IndexEntryIterator iterator(Key startRowKey, Key stopRowKey) {
	return new IndexEntryIterator() {

	    @Override
	    public Key getStartRowKey() {
		return startRowKey;
	    }

	    @Override
	    public Key getEndRowKey() {
		return stopRowKey;
	    }

	    @Override
	    protected IndexEntry findNext() {
		try {
		    return readEntry(dataFile, getStream());
		} catch (IOException e) {
		    logger.error("Could not read next index entry.", e);
		    return null;
		}
	    }

	};
    }

}
