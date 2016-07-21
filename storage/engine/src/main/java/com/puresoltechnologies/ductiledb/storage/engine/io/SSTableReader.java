package com.puresoltechnologies.ductiledb.storage.engine.io;

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

    public static File getIndexName(File dataFile) {
	return new File(dataFile.getParent(),
		dataFile.getName().replace(ColumnFamilyEngine.DATA_FILE_SUFFIX, ColumnFamilyEngine.INDEX_FILE_SUFFIX));
    }

    private final Storage storage;
    private final File dataFile;
    private final File indexFile;
    private final int bufferSize;

    public SSTableReader(Storage storage, File dataFile, int bufferSize) {
	this(storage, dataFile, getIndexName(dataFile), bufferSize);
    }

    public SSTableReader(Storage storage, File dataFile, File indexFile, int bufferSize) {
	this.storage = storage;
	this.dataFile = dataFile;
	this.indexFile = indexFile;
	this.bufferSize = bufferSize;
    }

    public SSTableIndexIterable readIndex() throws FileNotFoundException {
	return new SSTableIndexIterable(storage.open(indexFile), bufferSize);
    }

    public SSTableDataIterable readData() throws FileNotFoundException {
	return new SSTableDataIterable(storage.open(dataFile), bufferSize);
    }
}
