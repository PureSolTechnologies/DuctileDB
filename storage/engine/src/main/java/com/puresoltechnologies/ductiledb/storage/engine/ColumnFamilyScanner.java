package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.commons.misc.PeekingIterator;
import com.puresoltechnologies.ductiledb.storage.engine.index.IndexEntry;
import com.puresoltechnologies.ductiledb.storage.engine.index.IndexIterator;
import com.puresoltechnologies.ductiledb.storage.engine.index.RowKey;
import com.puresoltechnologies.ductiledb.storage.engine.io.DataFileSet;
import com.puresoltechnologies.ductiledb.storage.engine.io.data.DataFileReader;
import com.puresoltechnologies.ductiledb.storage.engine.io.index.IndexEntryIterable;
import com.puresoltechnologies.ductiledb.storage.engine.memtable.Memtable;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class ColumnFamilyScanner implements PeekingIterator<IndexEntry>, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyScanner.class);

    private final IndexIterator memtableIterator;
    private final List<File> commitLogs;
    private final List<DataFileReader> dataFileReaders = new ArrayList<>();
    private final List<IndexEntryIterable> indexIterables = new ArrayList<>();
    private final List<IndexIterator> indexIterators = new ArrayList<>();
    private final DataFileSet dataFiles;
    private final RowKey startRowKey;
    private final RowKey endRowKey;

    public ColumnFamilyScanner(Storage storage, Memtable memtable, List<File> commitLogs, DataFileSet dataFiles,
	    RowKey startRowKey, RowKey endRowKey) throws FileNotFoundException {
	super();
	this.memtableIterator = memtable.iterator(startRowKey, endRowKey);
	this.commitLogs = commitLogs;
	this.dataFiles = dataFiles;
	this.startRowKey = startRowKey;
	this.endRowKey = endRowKey;

	for (File commitLog : commitLogs) {
	    dataFileReaders.add(new DataFileReader(storage, commitLog));
	    File indexFile = DataFileSet.getIndexName(commitLog);
	    IndexEntryIterable indexIterable = new IndexEntryIterable(indexFile, storage.open(indexFile));
	    indexIterables.add(indexIterable);
	    indexIterators.add(indexIterable.iterator());
	}

    }

    @Override
    public void close() throws IOException {
	dataFileReaders.forEach(reader -> {
	    try {
		reader.close();
	    } catch (IOException e) {
		logger.warn("Could not close reader.", e);
	    }
	});
	indexIterables.forEach(iterable -> {
	    try {
		iterable.close();
	    } catch (IOException e) {
		logger.warn("Could not close index iterable.", e);
	    }
	});
    }

    @Override
    public boolean hasNext() {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public IndexEntry next() {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public IndexEntry peek() {
	// TODO Auto-generated method stub
	return null;
    }

}
