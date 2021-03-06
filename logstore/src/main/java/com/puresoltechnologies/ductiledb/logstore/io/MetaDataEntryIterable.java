package com.puresoltechnologies.ductiledb.logstore.io;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.commons.Bytes;
import com.puresoltechnologies.ductiledb.logstore.Key;

public class MetaDataEntryIterable extends InputStreamIterable<DuctileDBInputStream, MetaDataEntry> {

    private static final Logger logger = LoggerFactory.getLogger(MetaDataEntryIterable.class);

    private final int fileCount;

    public MetaDataEntryIterable(DuctileDBInputStream inputStream) throws IOException {
	super(inputStream);
	this.fileCount = readFileCount();
    }

    private int readFileCount() throws IOException {
	DuctileDBInputStream inputStream = getInputStream();
	byte[] buffer = new byte[4];
	inputStream.read(buffer);
	return Bytes.toInt(buffer);
    }

    public int getFileCount() {
	return fileCount;
    }

    @Override
    protected MetaDataEntry readEntry(DuctileDBInputStream inputStream) {
	try {
	    byte[] buffer = new byte[8];
	    int len = inputStream.read(buffer, 0, 4);
	    if (len < 0) {
		return null;
	    } else if (len < 4) {
		logger.warn("Could not read full number of bytes needed. It is maybe a broken metadata file.");
		return null;
	    }
	    int fileNameLength = Bytes.toInt(buffer);
	    byte[] fileNameBuffer = new byte[fileNameLength];
	    len = inputStream.read(fileNameBuffer);
	    if (len < fileNameLength) {
		logger.warn("Could not read full number of bytes needed. It is maybe a broken metadata file.");
		return null;
	    }
	    String fileName = Bytes.toString(fileNameBuffer);

	    len = inputStream.read(buffer, 0, 4);
	    if (len < 0) {
		// We found an empty data file and return an meta data enty with
		// it.
		return new MetaDataEntry(fileName, null, -1, null, -1);
	    } else if (len < 4) {
		logger.warn("Could not read full number of bytes needed. It is maybe a broken metadata file.");
		return null;
	    }
	    int keyLength = Bytes.toInt(buffer);
	    byte[] startKey = new byte[keyLength];
	    len = inputStream.read(startKey);
	    if (len < keyLength) {
		logger.warn("Could not read full number of bytes needed. It is maybe a broken metadata file.");
		return null;
	    }

	    len = inputStream.read(buffer, 0, 8);
	    if (len < 8) {
		logger.warn("Could not read full number of bytes needed. It is maybe a broken metadata file.");
		return null;
	    }
	    long startOffset = Bytes.toLong(buffer);

	    len = inputStream.read(buffer, 0, 4);
	    if (len < 4) {
		logger.warn("Could not read full number of bytes needed. It is maybe a broken metadata file.");
		return null;
	    }
	    keyLength = Bytes.toInt(buffer);
	    byte[] endKey = new byte[keyLength];
	    len = inputStream.read(endKey);
	    if (len < keyLength) {
		logger.warn("Could not read full number of bytes needed. It is maybe a broken metadata file.");
		return null;
	    }

	    len = inputStream.read(buffer, 0, 8);
	    if (len < 8) {
		logger.warn("Could not read full number of bytes needed. It is maybe a broken metadata file.");
		return null;
	    }
	    long endOffset = Bytes.toLong(buffer);

	    return new MetaDataEntry(fileName, Key.of(startKey), startOffset, Key.of(endKey), endOffset);
	} catch (IOException e) {
	    logger.error("Error reading metadata file", e);
	    return null;
	}
    }

}
