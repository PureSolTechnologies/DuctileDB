package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.commons.misc.PeekingIterator;
import com.puresoltechnologies.ductiledb.storage.engine.index.IndexEntry;
import com.puresoltechnologies.ductiledb.storage.engine.index.IndexIterator;
import com.puresoltechnologies.ductiledb.storage.engine.index.Memtable;
import com.puresoltechnologies.ductiledb.storage.engine.index.RowKey;
import com.puresoltechnologies.ductiledb.storage.engine.io.DataFileSet;
import com.puresoltechnologies.ductiledb.storage.engine.io.data.DataFileReader;
import com.puresoltechnologies.ductiledb.storage.engine.io.index.IndexEntryIterable;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class ColumnFamilyScanner implements PeekingIterator<ColumnFamilyRow>, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyScanner.class);

    private final Storage storage;
    private final IndexIterator memtableIterator;
    private final Map<File, DataFileReader> dataFileReaders = new HashMap<>();
    private final List<IndexEntryIterable> commitLogIndexIterables = new ArrayList<>();
    private final List<IndexIterator> commitLogIndexIterators = new ArrayList<>();
    private final DataFileSet dataFiles;
    private final IndexIterator dataFilesIndexIterator;
    private final RowKey startRowKey;
    private final RowKey endRowKey;
    private ColumnFamilyRow nextRow = null;

    public ColumnFamilyScanner(Storage storage, Memtable memtable, List<File> commitLogs, DataFileSet dataFiles,
	    RowKey startRowKey, RowKey endRowKey) throws FileNotFoundException {
	super();
	this.storage = storage;
	this.memtableIterator = memtable.iterator(startRowKey, endRowKey);
	this.dataFiles = dataFiles;
	this.startRowKey = startRowKey;
	this.endRowKey = endRowKey;
	this.dataFilesIndexIterator = dataFiles.getIndexIterator(startRowKey, endRowKey);

	for (File commitLog : commitLogs) {
	    File indexFile = DataFileSet.getIndexName(commitLog);
	    IndexEntryIterable indexIterable = new IndexEntryIterable(indexFile, storage.open(indexFile));
	    commitLogIndexIterables.add(indexIterable);
	    IndexIterator iterator = indexIterable.iterator();
	    iterator.gotoStart(startRowKey);
	    commitLogIndexIterators.add(iterator);
	}

    }

    @Override
    public void close() throws IOException {
	dataFileReaders.values().forEach(reader -> {
	    try {
		reader.close();
	    } catch (IOException e) {
		logger.warn("Could not close reader.", e);
	    }
	});
	dataFileReaders.clear();
	commitLogIndexIterables.forEach(iterable -> {
	    try {
		iterable.close();
	    } catch (IOException e) {
		logger.warn("Could not close index iterable.", e);
	    }
	});
	commitLogIndexIterables.clear();
    }

    @Override
    public boolean hasNext() {
	if (nextRow == null) {
	    readNextRow();
	}
	return nextRow != null;
    }

    @Override
    public ColumnFamilyRow next() {
	if (nextRow == null) {
	    readNextRow();
	}
	ColumnFamilyRow result = nextRow;
	nextRow = null;
	return result;
    }

    @Override
    public ColumnFamilyRow peek() {
	if (nextRow == null) {
	    readNextRow();
	}
	return nextRow;
    }

    private void readNextRow() {
	IndexEntry minimum = null;
	if (memtableIterator.hasNext()) {
	    minimum = memtableIterator.peek();
	}
	for (IndexIterator iterator : commitLogIndexIterators) {
	    if (iterator.hasNext()) {
		int compareResult = minimum.compareTo(iterator.peek());
		if (compareResult == 0) {
		    iterator.skip();
		} else if (compareResult > 0) {
		    minimum = iterator.peek();
		}
	    } else {
		iterator.remove();
	    }
	}
	if (dataFilesIndexIterator.hasNext()) {
	    int compareResult = minimum.compareTo(dataFilesIndexIterator.peek());
	    if (compareResult == 0) {
		dataFilesIndexIterator.skip();
	    } else if (compareResult > 0) {
		minimum = dataFilesIndexIterator.peek();
	    }
	}
	if (minimum != null) {
	    DataFileReader fileReader = dataFileReaders.get(minimum.getDataFile());
	    if (fileReader == null) {
		try {
		    fileReader = new DataFileReader(storage, minimum.getDataFile());
		} catch (FileNotFoundException e) {
		    logger.error("Could not read file.", e);
		}
		dataFileReaders.put(minimum.getDataFile(), fileReader);
	    }
	    if (fileReader != null) {
		try {
		    nextRow = fileReader.getRow(minimum);
		} catch (IOException e) {
		    logger.error("Could not read file.", e);
		}
	    }
	    // TODO read the row from the index!

	    if (memtableIterator.hasNext()) {
		if (memtableIterator.peek().equals(minimum)) {
		    memtableIterator.skip();
		}
	    }
	    for (IndexIterator iterator : commitLogIndexIterators) {
		if (iterator.hasNext()) {
		    if (iterator.peek().equals(minimum)) {
			iterator.skip();
		    }
		}
	    }
	    if (dataFilesIndexIterator.hasNext()) {
		if (dataFilesIndexIterator.hasNext()) {
		    if (dataFilesIndexIterator.peek().equals(minimum)) {
			dataFilesIndexIterator.skip();
		    }
		}
	    }
	}
    }
}
