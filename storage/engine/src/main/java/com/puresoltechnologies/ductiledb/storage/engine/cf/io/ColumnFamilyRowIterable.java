package com.puresoltechnologies.ductiledb.storage.engine.cf.io;

import java.io.BufferedInputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnFamilyRow;
import com.puresoltechnologies.ductiledb.storage.engine.cf.index.primary.io.IndexEntryIterable;
import com.puresoltechnologies.ductiledb.storage.engine.io.InputStreamIterable;

public class ColumnFamilyRowIterable extends InputStreamIterable<ColumnFamilyRow> {

    private static final Logger logger = LoggerFactory.getLogger(IndexEntryIterable.class);

    public ColumnFamilyRowIterable(BufferedInputStream bufferedOutputStream) {
	this(new DataInputStream(bufferedOutputStream));
    }

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
