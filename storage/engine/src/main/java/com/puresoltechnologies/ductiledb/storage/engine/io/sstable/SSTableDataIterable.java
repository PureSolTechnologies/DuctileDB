package com.puresoltechnologies.ductiledb.storage.engine.io.sstable;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.io.InputStreamIterable;
import com.puresoltechnologies.ductiledb.storage.engine.memtable.ColumnMap;

public class SSTableDataIterable extends InputStreamIterable<SSTableDataEntry> {

    private static final Logger logger = LoggerFactory.getLogger(SSTableIndexIterable.class);

    public SSTableDataIterable(BufferedInputStream inputStream) {
	super(inputStream);
    }

    @Override
    protected SSTableDataEntry readEntry() {
	InputStream inputStream = getInputStream();
	try {
	    byte[] buffer = new byte[4];
	    int len = inputStream.read(buffer, 0, 4);
	    if (len == -1) {
		return null;
	    } else if (len < 4) {
		logger.warn("Could not read full number of bytes needed. It is maybe a broken data file.");
		return null;
	    }
	    int length = Bytes.toInt(buffer);
	    byte[] rowKey = new byte[length];
	    len = inputStream.read(rowKey);
	    if (len < length) {
		logger.warn("Could not read full number of bytes needed. It is maybe a broken data file.");
		return null;
	    }
	    len = inputStream.read(buffer, 0, 4);
	    if (len < 4) {
		logger.warn("Could not read full number of bytes needed. It is maybe a broken data file.");
		return null;
	    }
	    int columnCount = Bytes.toInt(buffer);
	    ColumnMap columns = new ColumnMap();
	    for (int i = 0; i < columnCount; ++i) {
		// Column key...
		len = inputStream.read(buffer, 0, 4);
		if (len < 4) {
		    logger.warn("Could not read full number of bytes needed. It is maybe a broken data file.");
		    return null;
		}
		length = Bytes.toInt(buffer);
		byte[] columnKey = new byte[length];
		len = inputStream.read(columnKey);
		if (len < length) {
		    logger.warn("Could not read full number of bytes needed. It is maybe a broken data file.");
		    return null;
		}
		// Column value...
		len = inputStream.read(buffer, 0, 4);
		if (len < 4) {
		    logger.warn("Could not read full number of bytes needed. It is maybe a broken data file.");
		    return null;
		}
		length = Bytes.toInt(buffer);
		byte[] columnValue = new byte[length];
		len = inputStream.read(columnValue);
		if (len < length) {
		    logger.warn("Could not read full number of bytes needed. It is maybe a broken data file.");
		    return null;
		}
		columns.put(columnKey, columnValue);
	    }
	    return new SSTableDataEntry(rowKey, columns);
	} catch (IOException e) {
	    logger.error("Error reading data file.", e);
	    return null;
	}
    }

}
