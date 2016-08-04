package com.puresoltechnologies.ductiledb.storage.engine.io.sstable;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.puresoltechnologies.ductiledb.storage.engine.ColumnFamilyRow;
import com.puresoltechnologies.ductiledb.storage.engine.index.RowKey;
import com.puresoltechnologies.ductiledb.storage.engine.io.FileReader;
import com.puresoltechnologies.ductiledb.storage.engine.memtable.ColumnMap;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class DataFileReader extends FileReader<DataInputStream> {

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
}
