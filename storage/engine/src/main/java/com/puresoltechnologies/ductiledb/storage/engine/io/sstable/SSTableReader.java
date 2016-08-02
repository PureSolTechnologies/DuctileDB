package com.puresoltechnologies.ductiledb.storage.engine.io.sstable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.index.IndexEntry;
import com.puresoltechnologies.ductiledb.storage.engine.index.RowKey;
import com.puresoltechnologies.ductiledb.storage.engine.memtable.ColumnMap;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This class is responsible for reading SSTables and there indizes.
 * 
 * @author Rick-Rainer Ludwig
 */
public class SSTableReader {

    private static final Logger logger = LoggerFactory.getLogger(SSTableReader.class);

    private final Storage storage;
    private final File dataFile;
    private final File indexFile;

    public SSTableReader(Storage storage, File dataFile) {
	this(storage, dataFile, SSTableSet.getIndexName(dataFile));
    }

    public SSTableReader(Storage storage, File dataFile, File indexFile) {
	this.storage = storage;
	this.dataFile = dataFile;
	this.indexFile = indexFile;
    }

    public SSTableIndexIterable readIndex() throws FileNotFoundException {
	return new SSTableIndexIterable(indexFile, new IndexInputStream(storage.open(indexFile)));
    }

    public ColumnFamilyRowIterable readData() throws FileNotFoundException {
	return new ColumnFamilyRowIterable(new DataInputStream(storage.open(dataFile)));
    }

    public ColumnMap readColumnMap(IndexEntry indexEntry) throws StorageException {
	try (ColumnFamilyRowIterable data = readData()) {
	    data.skip(indexEntry.getOffset());
	    ColumnFamilyRow entry = data.iterator().next();
	    return entry.getColumnMap();
	} catch (IOException e) {
	    throw new StorageException("Could not read data.", e);
	}
    }

    public ColumnMap readColumnMap(RowKey rowKey) throws StorageException {
	try (SSTableIndexIterable index = readIndex()) {
	    Iterator<IndexEntry> indexIterator = index.iterator();
	    while (indexIterator.hasNext()) {
		IndexEntry entry = indexIterator.next();
		if (entry.getRowKey().compareTo(rowKey) == 0) {
		    return readColumnMap(entry);
		}
	    }
	    return null;
	} catch (FileNotFoundException e) {
	    logger.warn("Could not read file.", e);
	    return null;
	} catch (IOException e) {
	    throw new StorageException("Could not read data.", e);
	}
    }

    public ColumnMap readColumnMap(RowKey rowKey, IndexEntry startOffset, IndexEntry endOffset)
	    throws StorageException {
	try (SSTableIndexIterable index = readIndex()) {
	    index.skip(startOffset.getOffset());
	    Iterator<IndexEntry> indexIterator = index.iterator();
	    while (indexIterator.hasNext()) {
		IndexEntry indexEntry = indexIterator.next();
		if (indexEntry.getRowKey().compareTo(rowKey) == 0) {
		    return readColumnMap(indexEntry);
		} else if (indexEntry.getRowKey().compareTo(endOffset.getRowKey()) > 0) {
		    return null;
		}
	    }
	    return null;
	} catch (IOException e) {
	    throw new StorageException("Could not read data.", e);
	}
    }

}
