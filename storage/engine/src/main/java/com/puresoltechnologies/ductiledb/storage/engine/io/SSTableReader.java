package com.puresoltechnologies.ductiledb.storage.engine.io;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This class is responsible for reading SSTables and there indizes.
 * 
 * @author Rick-Rainer Ludwig
 */
public class SSTableReader implements Closeable {

    private final Storage storage;
    private final File sstableFile;
    private final File indexFile;
    private final int bufferSize;

    public SSTableReader(Storage storage, File sstableFile, File indexFile, int bufferSize) {
	this.storage = storage;
	this.sstableFile = sstableFile;
	this.indexFile = indexFile;
	this.bufferSize = bufferSize;
    }

    @Override
    public void close() throws IOException {
	// TODO Auto-generated method stub
    }

    public SSTableIndexIterable readIndex() throws FileNotFoundException {
	return new SSTableIndexIterable(storage.open(indexFile), bufferSize);
    }
}
