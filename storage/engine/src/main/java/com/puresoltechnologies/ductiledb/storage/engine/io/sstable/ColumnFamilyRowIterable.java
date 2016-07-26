package com.puresoltechnologies.ductiledb.storage.engine.io.sstable;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.storage.engine.io.InputStreamIterable;

public class ColumnFamilyRowIterable extends InputStreamIterable<ColumnFamilyRow> {

    private static final Logger logger = LoggerFactory.getLogger(SSTableIndexIterable.class);

    public ColumnFamilyRowIterable(DataInputStream inputStream) {
	super(inputStream);
    }

    @Override
    protected ColumnFamilyRow readEntry() {
	DataInputStream inputStream = (DataInputStream) getInputStream();
	try {
	    return inputStream.readRow();
	} catch (IOException e) {
	    logger.error("Error reading data file.", e);
	    return null;
	}
    }

}
