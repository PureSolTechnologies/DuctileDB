package com.puresoltechnologies.ductiledb.logstore.io;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.commons.misc.PeekingIterator;
import com.puresoltechnologies.commons.misc.io.CloseableIterable;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.logstore.Row;
import com.puresoltechnologies.ductiledb.logstore.index.IndexEntry;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class DataFileReader extends FileReader<DataInputStream> implements CloseableIterable<Row> {

    private static final Logger logger = LoggerFactory.getLogger(DataFileReader.class);

    public DataFileReader(Storage storage, File dataFile) throws IOException {
	super(storage, dataFile);
    }

    @Override
    protected DataInputStream createStream(BufferedInputStream bufferedInputStream) {
	return new DataInputStream(bufferedInputStream);
    }

    public Row getRow() throws IOException {
	return getStream().readRow();
    }

    public Row getRow(IndexEntry indexEntry) throws IOException {
	return getRow(indexEntry.getOffset());
    }

    public Row getRow(long offset) throws IOException {
	goToOffset(offset);
	return getStream().readRow();
    }

    public Row getRow(Key rowKey) throws IOException {
	do {
	    Row row = getStream().readRow();
	    if ((row != null) && (row.getKey().equals(rowKey))) {
		return row;
	    }
	} while (!getStream().isEof());
	return null;
    }

    public Row getRow(Key rowKey, long startOffset, long endOffset) throws IOException {
	goToOffset(startOffset);
	do {
	    Row row = getStream().readRow();
	    if ((row != null) && (row.getKey().equals(rowKey))) {
		return row;
	    }
	} while ((getOffset() <= endOffset) & (!getStream().isEof()));
	return null;
    }

    @Override
    public PeekingIterator<Row> iterator() {
	return new PeekingIterator<Row>() {

	    private final DataInputStream stream = getStream();
	    private Row nextEntry = null;

	    @Override
	    public boolean hasNext() {
		if (nextEntry != null) {
		    return true;
		}
		try {
		    nextEntry = stream.readRow();
		} catch (IOException e) {
		    logger.error("Could not read column family row.", e);
		}
		return nextEntry != null;
	    }

	    @Override
	    public Row next() {
		if (nextEntry == null) {
		    try {
			return stream.readRow();
		    } catch (IOException e) {
			logger.error("Could not read column family row.", e);
			return null;
		    }
		} else {
		    Row result = nextEntry;
		    nextEntry = null;
		    return result;
		}
	    }

	    @Override
	    public Row peek() {
		if (nextEntry == null) {
		    try {
			nextEntry = stream.readRow();
		    } catch (IOException e) {
			logger.error("Could not read column family row.", e);
		    }
		}
		return nextEntry;
	    }
	};

    }
}
