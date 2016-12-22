package com.puresoltechnologies.ductiledb.engine.cf.index.secondary.io;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;

import com.puresoltechnologies.ductiledb.engine.Key;
import com.puresoltechnologies.ductiledb.engine.io.FileReader;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class SecondaryIndexFileReader extends FileReader<SecondaryIndexInputStream> {

    public SecondaryIndexFileReader(Storage storage, File indexFile) throws IOException {
	super(storage, indexFile);
    }

    @Override
    protected SecondaryIndexInputStream createStream(BufferedInputStream bufferedInputStream) throws IOException {
	return new SecondaryIndexInputStream(bufferedInputStream);
    }

    public SecondaryIndexEntry get() throws IOException {
	return getStream().readEntry();
    }

    public SecondaryIndexEntry get(long offset) throws IOException {
	goToOffset(offset);
	return getStream().readEntry();
    }

    public SecondaryIndexEntry get(Key rowKey) throws IOException {
	while (!getStream().isEof()) {
	    SecondaryIndexEntry indexEntry = getStream().readEntry();
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
