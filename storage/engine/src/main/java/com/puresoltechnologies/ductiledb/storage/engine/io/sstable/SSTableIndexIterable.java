package com.puresoltechnologies.ductiledb.storage.engine.io.sstable;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.storage.engine.io.InputStreamIterable;
import com.puresoltechnologies.ductiledb.storage.engine.utils.Bytes;

public class SSTableIndexIterable extends InputStreamIterable<SSTableIndexEntry> {

    private static final Logger logger = LoggerFactory.getLogger(SSTableIndexIterable.class);

    public SSTableIndexIterable(InputStream inputStream, int blockSize) {
	super(new BufferedInputStream(inputStream, blockSize));
    }

    @Override
    protected SSTableIndexEntry readEntry() {
	InputStream inputStream = getInputStream();
	try {
	    byte[] buffer = new byte[8];
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

}
