package com.puresoltechnologies.ductiledb.storage.engine.io;

import java.io.File;
import java.io.FileNotFoundException;

import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This class is used to read a commit log provided via InputStream.
 * 
 * @author Rick-Rainer Ludwig
 */
public class CommitLogReader {

    private final Storage storage;
    private final File commitLogFile;
    private final int blockSize;

    public CommitLogReader(Storage storage, File commitLogFile, int blockSize) {
	super();
	this.storage = storage;
	this.commitLogFile = commitLogFile;
	this.blockSize = blockSize;
    }

    public CommitLogIterable readData() throws FileNotFoundException {
	return new CommitLogIterable(storage.open(commitLogFile), blockSize);
    }
}
