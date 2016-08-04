package com.puresoltechnologies.ductiledb.storage.engine.io.data;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.commons.misc.PeekingIterator;
import com.puresoltechnologies.ductiledb.storage.engine.ColumnFamilyRow;
import com.puresoltechnologies.ductiledb.storage.engine.index.IndexEntry;
import com.puresoltechnologies.ductiledb.storage.engine.index.RowKey;
import com.puresoltechnologies.ductiledb.storage.engine.io.CloseableIterable;
import com.puresoltechnologies.ductiledb.storage.engine.io.FileReader;
import com.puresoltechnologies.ductiledb.storage.engine.memtable.ColumnMap;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class DataFileReader extends FileReader<DataInputStream> implements CloseableIterable<ColumnFamilyRow> {

    private static final Logger logger = LoggerFactory.getLogger(DataFileReader.class);

    public DataFileReader(Storage storage, File dataFile) throws FileNotFoundException {
	super(storage, dataFile);
    }

    @Override
    protected DataInputStream createStream(BufferedInputStream bufferedInputStream) {
	return new DataInputStream(bufferedInputStream);
    }

    public ColumnMap get() throws IOException {
	return getStream().readRow().getColumnMap();
    }

    public ColumnMap get(IndexEntry indexEntry) throws IOException {
	return get(indexEntry.getOffset());
    }

    public ColumnMap get(long offset) throws IOException {
	goToOffset(offset);
	return getStream().readRow().getColumnMap();
    }

    public ColumnMap get(RowKey rowKey) throws IOException {
	do {
	    ColumnFamilyRow row = getStream().readRow();
	    if (row.getRowKey().equals(rowKey)) {
		return row.getColumnMap();
	    }
	} while (!getStream().isEof());
	return null;
    }

    public ColumnMap get(RowKey rowKey, long startOffset, long endOffset) throws IOException {
	goToOffset(startOffset);
	do {
	    ColumnFamilyRow row = getStream().readRow();
	    if (row.getRowKey().equals(rowKey)) {
		return row.getColumnMap();
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
