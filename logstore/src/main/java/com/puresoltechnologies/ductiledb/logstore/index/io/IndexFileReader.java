package com.puresoltechnologies.ductiledb.logstore.index.io;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;

import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.logstore.index.IndexEntry;
import com.puresoltechnologies.ductiledb.logstore.io.FileReader;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class IndexFileReader extends FileReader<IndexInputStream> {

    public IndexFileReader(Storage storage, File indexFile) throws IOException {
	super(storage, indexFile);
    }

    @Override
    protected IndexInputStream createStream(BufferedInputStream bufferedInputStream) throws IOException {
	return new IndexInputStream(bufferedInputStream);
    }

    public IndexEntry get() throws IOException {
	return getStream().readEntry();
    }

    public IndexEntry get(long offset) throws IOException {
	goToOffset(offset);
	return getStream().readEntry();
    }

    public IndexEntry get(Key rowKey) throws IOException {
	while (!getStream().isEof()) {
	    IndexEntry indexEntry = getStream().readEntry();
	    int compareResult = indexEntry.getRowKey().compareTo(rowKey);
	    if (compareResult == 0) {
		return indexEntry;
	    } else if (compareResult > 0) {
		return null;
	    }
	}
	return null;
    }

}
