package com.puresoltechnologies.ductiledb.storage.engine.io;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.storage.engine.Key;
import com.puresoltechnologies.ductiledb.storage.engine.cf.index.primary.Index;
import com.puresoltechnologies.ductiledb.storage.engine.cf.index.primary.IndexEntry;
import com.puresoltechnologies.ductiledb.storage.engine.cf.index.primary.IndexIterator;
import com.puresoltechnologies.ductiledb.storage.engine.cf.index.primary.io.IndexEntryIterable;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

class SSTableIndexIterator implements IndexIterator {

    private static final Logger logger = LoggerFactory.getLogger(SSTableIndexIterator.class);

    private final DataFileSet dataFileSet;
    private final Key start;
    private final Key stop;
    private final Storage storage;
    private final Index index;
    private File currentDataFile;
    private File currentIndexFile;
    private IndexEntryIterable indexIterable;
    private IndexIterator indexIterator;
    private IndexEntry nextIndex = null;

    public SSTableIndexIterator(DataFileSet dataFileSet, Key start, Key stop) throws IOException {
	this.dataFileSet = dataFileSet;
	this.start = start;
	this.stop = stop;
	this.storage = dataFileSet.getStorage();
	this.index = dataFileSet.getIndex();
	IndexEntry floor = index.floor(start);
	if (floor != null) {
	    currentDataFile = floor.getDataFile();
	} else {
	    IndexEntry ceiling = index.ceiling(start);
	    if (ceiling != null) {
		currentDataFile = ceiling.getDataFile();
	    }
	}
	if (currentDataFile != null) {
	    currentIndexFile = DataFileSet.getIndexName(currentDataFile);
	    indexIterable = new IndexEntryIterable(storage.open(currentIndexFile));
	    indexIterator = indexIterable.iterator(start, stop);
	    gotoStart(start);
	}
    }

    @Override
    public Key getStartRowKey() {
	return start;
    }

    @Override
    public Key getEndRowKey() {
	return stop;
    }

    @Override
    public IndexEntry peek() {
	if (nextIndex == null) {
	    readNext();
	}
	return nextIndex;
    }

    @Override
    public boolean hasNext() {
	if (nextIndex == null) {
	    readNext();
	}
	return nextIndex != null;
    }

    @Override
    public IndexEntry next() {
	if (nextIndex == null) {
	    readNext();
	}
	IndexEntry result = nextIndex;
	nextIndex = null;
	return result;
    }

    private void readNext() {
	if (indexIterator == null) {
	    nextIndex = null;
	    return;
	}
	if (indexIterator.hasNext()) {
	    nextIndex = indexIterator.next();
	} else {
	    try {
		try {
		    indexIterable.close();
		} finally {
		    indexIterable = null;
		    indexIterator = null;
		}
		currentIndexFile = dataFileSet.getNextIndexFile(currentIndexFile);
		if (currentIndexFile != null) {
		    indexIterable = new IndexEntryIterable(storage.open(currentIndexFile));
		    indexIterator = indexIterable.iterator(start, stop);
		    nextIndex = indexIterator.next();
		}
	    } catch (IOException e) {
		logger.error("Could not find next index file.", e);
		nextIndex = null;
	    }
	}
	if ((nextIndex != null) && (stop != null) && (nextIndex.getRowKey().compareTo(stop) > 0)) {
	    nextIndex = null;
	}
    }

    @Override
    public void close() throws IOException {
	if (indexIterable != null) {
	    indexIterable.close();
	    indexIterable = null;
	}
    }

}
