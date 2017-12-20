package com.puresoltechnologies.ductiledb.logstore;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.logstore.data.DataFileReader;
import com.puresoltechnologies.ductiledb.logstore.data.DataFileSet;
import com.puresoltechnologies.ductiledb.logstore.index.IndexEntry;
import com.puresoltechnologies.ductiledb.logstore.index.IndexEntryIterator;
import com.puresoltechnologies.ductiledb.logstore.index.IndexFileReader;
import com.puresoltechnologies.ductiledb.logstore.index.Memtable;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class RowScannerImpl implements RowScanner {

    private static final Logger logger = LoggerFactory.getLogger(RowScannerImpl.class);

    private final Storage storage;
    private final IndexEntryIterator memtableIterator;
    private final Map<File, DataFileReader> dataFileReaders = new HashMap<>();
    private final List<IndexFileReader> commitLogIndexFileReaders = new ArrayList<>();
    private final List<IndexEntryIterator> commitLogIndexIterators = new ArrayList<>();
    private final DataFileSet dataFiles;
    private final IndexEntryIterator dataFilesIndexIterator;
    private final Key startRowKey;
    private final Key endRowKey;
    private Row nextRow = null;

    public RowScannerImpl(Storage storage, Memtable memtable, List<File> commitLogs, DataFileSet dataFiles,
	    Key startRowKey, Key endRowKey) throws IOException {
	super();
	this.storage = storage;
	this.memtableIterator = memtable.iterator(startRowKey, endRowKey);
	this.dataFiles = dataFiles;
	this.startRowKey = startRowKey;
	this.endRowKey = endRowKey;
	this.dataFilesIndexIterator = dataFiles.getIndexIterator(startRowKey, endRowKey);

	for (File commitLog : commitLogs) {
	    System.out.println("CommitLog:" + commitLog);
	    File indexFile = DataFileSet.getIndexName(commitLog);
	    IndexFileReader indexFileReader = new IndexFileReader(storage, indexFile);
	    commitLogIndexFileReaders.add(indexFileReader);
	    IndexEntryIterator iterator = indexFileReader.iterator();
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
	commitLogIndexFileReaders.forEach(iterable -> {
	    try {
		iterable.close();
	    } catch (IOException e) {
		logger.warn("Could not close index iterable.", e);
	    }
	});
	commitLogIndexFileReaders.clear();
    }

    @Override
    public boolean hasNext() {
	if (nextRow == null) {
	    readNextRow();
	}
	if ((nextRow != null) && (endRowKey != null)) {
	    return endRowKey.compareTo(nextRow.getKey()) >= 0;
	}
	return (nextRow != null);
    }

    @Override
    public Row next() {
	if (nextRow == null) {
	    readNextRow();
	}
	if ((endRowKey != null) && (nextRow != null) && (endRowKey.compareTo(nextRow.getKey()) < 0)) {
	    return null;
	}
	Row result = nextRow;
	nextRow = null;
	return result;
    }

    @Override
    public Row peek() {
	if (nextRow == null) {
	    readNextRow();
	}
	if ((nextRow != null) && (endRowKey != null) && (endRowKey.compareTo(nextRow.getKey()) < 0)) {
	    return null;
	}
	return nextRow;
    }

    private void readNextRow() {
	nextRow = null;
	IndexEntry minimum = null;
	do {
	    minimum = null;
	    if (memtableIterator.hasNext()) {
		minimum = memtableIterator.peek();
	    }
	    Set<IndexEntryIterator> toBeDeleted = new HashSet<>();
	    for (IndexEntryIterator iterator : commitLogIndexIterators) {
		if (iterator.hasNext()) {
		    int compareResult = minimum != null ? minimum.compareTo(iterator.peek()) : 1;
		    if (compareResult == 0) {
			iterator.skip();
		    } else if (compareResult > 0) {
			minimum = iterator.peek();
		    }
		} else {
		    toBeDeleted.add(iterator);
		}
	    }
	    toBeDeleted.forEach(entry -> commitLogIndexIterators.remove(entry));
	    if (dataFilesIndexIterator.hasNext()) {
		int compareResult = minimum != null ? minimum.compareTo(dataFilesIndexIterator.peek()) : -1;
		if (compareResult == 0) {
		    dataFilesIndexIterator.skip();
		} else if (compareResult < 0) {
		    minimum = dataFilesIndexIterator.peek();
		}
	    }
	    if (minimum != null) {
		readNextEntryFromIndexEntry(minimum);
		if (memtableIterator.hasNext()) {
		    if (memtableIterator.peek().equals(minimum)) {
			memtableIterator.skip();
		    }
		}
		for (IndexEntryIterator iterator : commitLogIndexIterators) {
		    if (iterator.hasNext()) {
			if (iterator.peek().equals(minimum)) {
			    iterator.skip();
			}
		    }
		}
		if (dataFilesIndexIterator.hasNext()) {
		    if (dataFilesIndexIterator.peek().equals(minimum)) {
			dataFilesIndexIterator.skip();
		    }
		}
	    }
	} while ((minimum != null) && ((nextRow == null) || ((nextRow != null) && (nextRow.wasDeleted()))));
    }

    private void readNextEntryFromIndexEntry(IndexEntry minimum) {
	DataFileReader fileReader = dataFileReaders.get(minimum.getDataFile());
	if (fileReader == null) {
	    try {
		fileReader = new DataFileReader(storage, minimum.getDataFile());
	    } catch (IOException e) {
		logger.error("Could not read file.", e);
	    }
	    dataFileReaders.put(minimum.getDataFile(), fileReader);
	}
	if (fileReader != null) {
	    try {
		fileReader.seek(minimum.getOffset());
		Row row = fileReader.readRow(minimum);
		if (!row.wasDeleted()) {
		    nextRow = row;
		}
	    } catch (IOException e) {
		logger.error("Could not read file.", e);
	    }
	}
    }
}
