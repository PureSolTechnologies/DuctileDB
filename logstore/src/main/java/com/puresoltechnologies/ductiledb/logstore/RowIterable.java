package com.puresoltechnologies.ductiledb.logstore;

import java.io.BufferedInputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.logstore.index.io.IndexEntryIterable;
import com.puresoltechnologies.ductiledb.logstore.io.DataInputStream;
import com.puresoltechnologies.ductiledb.logstore.io.InputStreamIterable;

public class RowIterable extends InputStreamIterable<DataInputStream, Row> {

    private static final Logger logger = LoggerFactory.getLogger(IndexEntryIterable.class);

    public RowIterable(BufferedInputStream bufferedOutputStream) {
	this(new DataInputStream(bufferedOutputStream));
    }

    public RowIterable(DataInputStream inputStream) {
	super(inputStream);
    }

    @Override
    protected Row readEntry(DataInputStream inputStream) {
	try {
	    return inputStream.readRow();
	} catch (IOException e) {
	    logger.error("Error reading data file.", e);
	    return null;
	}
    }

}
