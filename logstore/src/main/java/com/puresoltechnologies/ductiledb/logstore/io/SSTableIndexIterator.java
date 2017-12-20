package com.puresoltechnologies.ductiledb.logstore.io;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.logstore.data.DataFileSet;
import com.puresoltechnologies.ductiledb.logstore.index.Index;
import com.puresoltechnologies.ductiledb.logstore.index.IndexEntry;
import com.puresoltechnologies.ductiledb.logstore.index.IndexEntryIterator;
import com.puresoltechnologies.ductiledb.logstore.index.IndexFileReader;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class SSTableIndexIterator extends IndexEntryIterator implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(SSTableIndexIterator.class);

    private final DataFileSet dataFileSet;
    private final Key start;
    private final Key stop;
    private final Storage storage;
    private final Index index;
    private File currentDataFile;
    private File currentIndexFile;
    private IndexFileReader indexFileReader;
    private IndexEntryIterator indexIterator;

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
	    indexFileReader = new IndexFileReader(storage, currentIndexFile);
	    indexIterator = indexFileReader.iterator(start, stop);
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
    protected IndexEntry findNext() {
	if (indexIterator == null) {
	    return null;
	}
	IndexEntry nextIndex = null;
	if (indexIterator.hasNext()) {
	    nextIndex = indexIterator.next();
	} else {
	    try {
		try {
		    indexFileReader.close();
		} finally {
		    indexFileReader = null;
		    indexIterator = null;
		}
		currentIndexFile = dataFileSet.getNextIndexFile(currentIndexFile);
		if (currentIndexFile != null) {
		    indexFileReader = new IndexFileReader(storage, currentIndexFile);
		    indexIterator = indexFileReader.iterator(start, stop);
		    nextIndex = indexIterator.next();
		}
	    } catch (IOException e) {
		logger.error("Could not find next index file.", e);
		return null;
	    }
	}
	if ((nextIndex != null) && (stop != null) && (nextIndex.getRowKey().compareTo(stop) > 0)) {
	    return null;
	}
	return nextIndex;
    }

    @Override
    public void close() throws IOException {
	if (indexFileReader != null) {
	    indexFileReader.close();
	    indexFileReader = null;
	}
    }

}
