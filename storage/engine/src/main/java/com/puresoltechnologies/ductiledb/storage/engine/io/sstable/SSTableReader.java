package com.puresoltechnologies.ductiledb.storage.engine.io.sstable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.ColumnFamilyEngine;
import com.puresoltechnologies.ductiledb.storage.engine.index.IndexEntry;
import com.puresoltechnologies.ductiledb.storage.engine.memtable.ColumnMap;
import com.puresoltechnologies.ductiledb.storage.engine.utils.ByteArrayComparator;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This class is responsible for reading SSTables and there indizes.
 * 
 * @author Rick-Rainer Ludwig
 */
public class SSTableReader {

    private static final ByteArrayComparator comparator = ByteArrayComparator.getInstance();

    private final Storage storage;
    private final File dataFile;
    private final File indexFile;

    public SSTableReader(Storage storage, File dataFile) {
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

    public ColumnMap readColumnMap(SSTableIndexEntry indexEntry) throws StorageException {
	try (SSTableDataIterable data = readData()) {
	    data.skip(indexEntry.getOffset());
	    SSTableDataEntry entry = data.iterator().next();
	    return entry.getColumns();
	} catch (IOException e) {
	    throw new StorageException("Could not read data.", e);
	}
    }

    public ColumnMap readColumnMap(byte[] rowKey) throws StorageException {
	try (SSTableIndexIterable index = readIndex()) {
	    Iterator<SSTableIndexEntry> indexIterator = index.iterator();
	    while (indexIterator.hasNext()) {
		SSTableIndexEntry entry = indexIterator.next();
		if (comparator.compare(entry.getRowKey(), rowKey) == 0) {
		    return readColumnMap(entry);
		}
	    }
	    return null;
	} catch (IOException e) {
	    throw new StorageException("Could not read data.", e);
	}
    }

    public ColumnMap readColumnMap(byte[] rowKey, IndexEntry startOffset, IndexEntry endOffset)
	    throws StorageException {
	try (SSTableIndexIterable index = readIndex()) {
	    index.skip(startOffset.getOffset());
	    Iterator<SSTableIndexEntry> indexIterator = index.iterator();
	    while (indexIterator.hasNext()) {
		SSTableIndexEntry indexEntry = indexIterator.next();
		if (comparator.compare(indexEntry.getRowKey(), rowKey) == 0) {
		    return readColumnMap(indexEntry);
		} else if (comparator.compare(indexEntry.getRowKey(), endOffset.getRowKey()) > 0) {
		    return null;
		}
	    }
	    return null;
	} catch (IOException e) {
	    throw new StorageException("Could not read data.", e);
	}
    }

}
