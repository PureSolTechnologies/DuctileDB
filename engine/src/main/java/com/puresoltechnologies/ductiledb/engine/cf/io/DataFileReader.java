package com.puresoltechnologies.ductiledb.engine.cf.io;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.commons.misc.PeekingIterator;
import com.puresoltechnologies.commons.misc.io.CloseableIterable;
import com.puresoltechnologies.ductiledb.engine.Key;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnFamilyRow;
import com.puresoltechnologies.ductiledb.engine.cf.index.primary.IndexEntry;
import com.puresoltechnologies.ductiledb.engine.io.FileReader;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class DataFileReader extends FileReader<DataInputStream> implements CloseableIterable<ColumnFamilyRow> {

    private static final Logger logger = LoggerFactory.getLogger(DataFileReader.class);

    public DataFileReader(Storage storage, File dataFile) throws IOException {
	super(storage, dataFile);
    }

    @Override
    protected DataInputStream createStream(BufferedInputStream bufferedInputStream) {
	return new DataInputStream(bufferedInputStream);
    }

    public ColumnFamilyRow getRow() throws IOException {
	return getStream().readRow();
    }

    public ColumnFamilyRow getRow(IndexEntry indexEntry) throws IOException {
	return getRow(indexEntry.getOffset());
    }

    public ColumnFamilyRow getRow(long offset) throws IOException {
	goToOffset(offset);
	return getStream().readRow();
    }

    public ColumnFamilyRow getRow(Key rowKey) throws IOException {
	do {
	    ColumnFamilyRow row = getStream().readRow();
	    if ((row != null) && (row.getRowKey().equals(rowKey))) {
		return row;
	    }
	} while (!getStream().isEof());
	return null;
    }

    public ColumnFamilyRow getRow(Key rowKey, long startOffset, long endOffset) throws IOException {
	goToOffset(startOffset);
	do {
	    ColumnFamilyRow row = getStream().readRow();
	    if ((row != null) && (row.getRowKey().equals(rowKey))) {
		return row;
	    }
	} while ((getOffset() <= endOffset) & (!getStream().isEof()));
	return null;
    }

    @Override
    public PeekingIterator<ColumnFamilyRow> iterator() {
	return new PeekingIterator<ColumnFamilyRow>() {

	    private final DataInputStream stream = getStream();
	    private ColumnFamilyRow nextEntry = null;

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
	    public ColumnFamilyRow next() {
		if (nextEntry == null) {
		    try {
			return stream.readRow();
		    } catch (IOException e) {
			logger.error("Could not read column family row.", e);
			return null;
		    }
		} else {
		    ColumnFamilyRow result = nextEntry;
		    nextEntry = null;
		    return result;
		}
	    }

	    @Override
	    public ColumnFamilyRow peek() {
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
