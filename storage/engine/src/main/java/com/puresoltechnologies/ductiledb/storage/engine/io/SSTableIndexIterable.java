package com.puresoltechnologies.ductiledb.storage.engine.io;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.storage.engine.utils.Bytes;

public class SSTableIndexIterable implements ClosableIterable<SSTableIndexEntry> {

    private static final Logger logger = LoggerFactory.getLogger(SSTableIndexIterable.class);

    private final InputStream inputStream;

    public SSTableIndexIterable(InputStream inputStream, int bufferSize) {
	super();
	this.inputStream = new BufferedInputStream(inputStream, bufferSize);
    }

    @Override
    public void close() throws IOException {
	inputStream.close();
    }

    @Override
    public Iterator<SSTableIndexEntry> iterator() {
	return new Iterator<SSTableIndexEntry>() {

	    private SSTableIndexEntry nextEntry = null;

	    @Override
	    public boolean hasNext() {
		if (nextEntry != null) {
		    return true;
		}
		nextEntry = readEntry();
		return nextEntry != null;
	    }

	    @Override
	    public SSTableIndexEntry next() {
		if (nextEntry == null) {
		    return readEntry();
		} else {
		    SSTableIndexEntry result = nextEntry;
		    nextEntry = null;
		    return result;
		}
	    }

	    private SSTableIndexEntry readEntry() {
		try {
		    byte[] buffer = new byte[4];
		    int len = inputStream.read(buffer, 0, 4);
		    if (len == -1) {
			return null;
		    } else if (len < 4) {
			logger.warn("Could not read full number of bytes needed. It is maybe a broken index file.");
			return null;
		    }
		    int keyLength = Bytes.toInt(buffer);
		    byte[] rowKey = new byte[keyLength];
		    len = inputStream.read(rowKey);
		    if (len < keyLength) {
			logger.warn("Could not read full number of bytes needed. It is maybe a broken index file.");
			return null;
		    }
		    buffer = new byte[8];
		    len = inputStream.read(buffer, 0, 8);
		    if (len < 8) {
			logger.warn("Could not read full number of bytes needed. It is maybe a broken index file.");
			return null;
		    }
		    long offset = Bytes.toLong(buffer);
		    return new SSTableIndexEntry(rowKey, offset);
		} catch (IOException e) {
		    logger.error("Error reading index file.", e);
		    return null;
		}
	    }
	};
    }

}
