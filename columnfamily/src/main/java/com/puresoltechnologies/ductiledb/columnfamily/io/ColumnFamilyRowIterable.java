package com.puresoltechnologies.ductiledb.columnfamily.io;

import java.io.BufferedInputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamilyRow;
import com.puresoltechnologies.ductiledb.logstore.index.io.IndexEntryIterable;
import com.puresoltechnologies.ductiledb.logstore.io.DataInputStream;
import com.puresoltechnologies.ductiledb.logstore.io.InputStreamIterable;

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
	    return ColumnFamilyRow.fromRow(inputStream.readRow());
	} catch (IOException e) {
	    logger.error("Error reading data file.", e);
	    return null;
	}
    }

}
