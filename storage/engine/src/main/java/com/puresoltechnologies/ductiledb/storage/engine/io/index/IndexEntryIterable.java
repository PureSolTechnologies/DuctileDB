package com.puresoltechnologies.ductiledb.storage.engine.io.index;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.commons.misc.PeekingIterator;
import com.puresoltechnologies.ductiledb.storage.engine.index.IndexEntry;
import com.puresoltechnologies.ductiledb.storage.engine.index.IndexIterator;
import com.puresoltechnologies.ductiledb.storage.engine.index.RowKey;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.io.DuctileDBInputStream;
import com.puresoltechnologies.ductiledb.storage.engine.io.InputStreamIterable;

public class IndexEntryIterable extends InputStreamIterable<IndexEntry> {

    private static final Logger logger = LoggerFactory.getLogger(IndexEntryIterable.class);

    private final IndexInputStream indexInputStream;

    public IndexEntryIterable(File indexFile, BufferedInputStream bufferedInputStream) {
	this(new IndexInputStream(indexFile, bufferedInputStream));
    }

    public IndexEntryIterable(IndexInputStream indexInputStream) {
	super(indexInputStream);
	this.indexInputStream = indexInputStream;
    }

    @Override
    protected IndexEntry readEntry() {
	DuctileDBInputStream inputStream = getInputStream();
	try {
	    byte[] buffer = new byte[8];
	    int len = inputStream.read(buffer, 0, 4);
	    if (len == -1) {
		return null;
	    } else if (len < 4) {
		logger.warn("Could not read full number of bytes needed. It is maybe a broken index file.");
		return null;
	    }
	    int keyLength = Bytes.toInt(buffer);
	    byte[] rowKey = new byte[keyLength];
	    len = inputStream.read(rowKey);
	    if (len < keyLength) {
		logger.warn("Could not read full number of bytes needed. It is maybe a broken index file.");
		return null;
	    }
	    len = inputStream.read(buffer, 0, 8);
	    if (len < 8) {
		logger.warn("Could not read full number of bytes needed. It is maybe a broken index file.");
		return null;
	    }
	    long offset = Bytes.toLong(buffer);
	    return new IndexEntry(new RowKey(rowKey), indexInputStream.getIndexFile(), offset);
	} catch (IOException e) {
	    logger.error("Error reading index file.", e);
	    return null;
	}
    }

    @Override
    public IndexIterator iterator() {
	return new IndexIterator() {

	    private final PeekingIterator<IndexEntry> iterator = IndexEntryIterable.super.iterator();

	    @Override
	    public RowKey getStartRowKey() {
		return null;
	    }

	    @Override
	    public RowKey getEndRowKey() {
		return null;
	    }

	    @Override
	    public boolean hasNext() {
		return iterator.hasNext();
	    }

	    @Override
	    public IndexEntry next() {
		return iterator.next();
	    }

	    @Override
	    public IndexEntry peek() {
		return iterator.peek();
	    }

	    @Override
	    public void close() throws IOException {
		// TODO Auto-generated method stub

	    }

	};
    }

    public IndexIterator iterator(RowKey startRowKey, RowKey stopRowKey) {
	return new IndexIterator() {
	    private final PeekingIterator<IndexEntry> iterator = IndexEntryIterable.super.iterator();

	    @Override
	    public RowKey getStartRowKey() {
		return startRowKey;
	    }

	    @Override
	    public RowKey getEndRowKey() {
		return stopRowKey;
	    }

	    @Override
	    public boolean hasNext() {
		return iterator.hasNext();
	    }

	    @Override
	    public IndexEntry next() {
		return iterator.next();
	    }

	    @Override
	    public IndexEntry peek() {
		return iterator.peek();
	    }

	    @Override
	    public void close() throws IOException {
		// TODO Auto-generated method stub

	    }
	};
    }

}
