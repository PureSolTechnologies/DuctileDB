package com.puresoltechnologies.ductiledb.storage.engine.cf.index.secondary.io;

import java.io.BufferedInputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.storage.engine.Key;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.io.DuctileDBInputStream;

public class SecondaryIndexInputStream extends DuctileDBInputStream {

    private static final Logger logger = LoggerFactory.getLogger(SecondaryIndexInputStream.class);

    public SecondaryIndexInputStream(BufferedInputStream bufferedOutputStream) throws IOException {
	super(bufferedOutputStream);
    }

    public SecondaryIndexEntry readEntry() throws IOException {
	byte[] buffer = new byte[4];
	int len = read(buffer, 0, 4);
	if (len == -1) {
	    return null;
	} else if (len < 4) {
	    logger.warn("Could not read full number of bytes needed. It is maybe a broken index file.");
	    return null;
	}
	int valueLength = Bytes.toInt(buffer);
	byte[] value = new byte[valueLength];
	len = read(value);
	if (len < valueLength) {
	    logger.warn("Could not read full number of bytes needed. It is maybe a broken index file.");
	    return null;
	}
	len = read(buffer, 0, 4);
	if (len < 4) {
	    logger.warn("Could not read full number of bytes needed. It is maybe a broken index file.");
	    return null;
	}
	int keyLength = Bytes.toInt(buffer);
	byte[] key = new byte[keyLength];
	len = read(key);
	if (len < keyLength) {
	    logger.warn("Could not read full number of bytes needed. It is maybe a broken index file.");
	    return null;
	}
	return new SecondaryIndexEntry(value, Key.of(key));
    }

}
