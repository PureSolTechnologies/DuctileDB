package com.puresoltechnologies.ductiledb.storage.engine.io.sstable;

import java.io.File;
import java.io.FileNotFoundException;

import com.puresoltechnologies.ductiledb.storage.engine.ColumnFamilyEngine;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This class is responsible for reading SSTables and there indizes.
 * 
 * @author Rick-Rainer Ludwig
 */
public class SSTableReader {

    private final Storage storage;
    private final File dataFile;
    private final File indexFile;

    public SSTableReader(Storage storage, File dataFile, int blockSize) {
	this(storage, dataFile, ColumnFamilyEngine.getIndexName(dataFile));
    }

    public SSTableReader(Storage storage, File dataFile, File indexFile) {
	this.storage = storage;
	this.dataFile = dataFile;
	this.indexFile = indexFile;
    }

    public SSTableIndexIterable readIndex() throws FileNotFoundException {
	return new SSTableIndexIterable(storage.open(indexFile));
    }

    public SSTableDataIterable readData() throws FileNotFoundException {
	return new SSTableDataIterable(storage.open(dataFile));
    }
}
