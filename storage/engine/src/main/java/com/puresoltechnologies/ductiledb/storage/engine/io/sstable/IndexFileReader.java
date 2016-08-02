package com.puresoltechnologies.ductiledb.storage.engine.io.sstable;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.puresoltechnologies.ductiledb.storage.engine.index.IndexEntry;
import com.puresoltechnologies.ductiledb.storage.engine.io.FileReader;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class IndexFileReader extends FileReader<IndexInputStream> {

    public IndexFileReader(Storage storage, File indexFile) throws FileNotFoundException {
	super(storage, indexFile);
    }

    @Override
    protected IndexInputStream createStream(BufferedInputStream bufferedInputStream) {
	return new IndexInputStream(getFile(), bufferedInputStream);
    }

    public IndexEntry get() throws IOException {
	return getStream().readEntry();
    }

    public IndexEntry get(long offset) throws IOException {
	goToOffset(offset);
	return getStream().readEntry();
    }

}
