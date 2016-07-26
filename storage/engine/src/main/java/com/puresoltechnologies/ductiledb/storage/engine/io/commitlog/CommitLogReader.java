package com.puresoltechnologies.ductiledb.storage.engine.io.commitlog;

import java.io.File;
import java.io.FileNotFoundException;

import com.puresoltechnologies.ductiledb.storage.engine.io.DuctileDBInputStream;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This class is used to read a commit log provided via InputStream.
 * 
 * @author Rick-Rainer Ludwig
 */
public class CommitLogReader {

    private final Storage storage;
    private final File commitLogFile;

    public CommitLogReader(Storage storage, File commitLogFile) {
	super();
	this.storage = storage;
	this.commitLogFile = commitLogFile;
    }

    public CommitLogIterable readData() throws FileNotFoundException {
	return new CommitLogIterable(new DuctileDBInputStream(storage.open(commitLogFile)));
    }
}
