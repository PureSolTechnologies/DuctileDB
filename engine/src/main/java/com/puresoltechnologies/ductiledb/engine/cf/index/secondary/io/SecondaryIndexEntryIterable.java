package com.puresoltechnologies.ductiledb.engine.cf.index.secondary.io;

import java.io.BufferedInputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.commons.misc.PeekingIterator;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.logstore.io.InputStreamIterable;

public class SecondaryIndexEntryIterable extends InputStreamIterable<SecondaryIndexEntry> {

    private static final Logger logger = LoggerFactory.getLogger(SecondaryIndexEntryIterable.class);

    private final SecondaryIndexInputStream indexInputStream;

    public SecondaryIndexEntryIterable(BufferedInputStream bufferedInputStream) throws IOException {
	this(new SecondaryIndexInputStream(bufferedInputStream));
    }

    public SecondaryIndexEntryIterable(SecondaryIndexInputStream indexInputStream) {
	super(indexInputStream);
	this.indexInputStream = indexInputStream;
    }

    @Override
    protected SecondaryIndexEntry readEntry() {
	try {
	    return indexInputStream.readEntry();
	} catch (IOException e) {
	    logger.error("Error reading index file.", e);
	    return null;
	}
    }

    @Override
    public SecondaryIndexIterator iterator() {
	return new SecondaryIndexIterator() {

	    private final PeekingIterator<SecondaryIndexEntry> iterator = SecondaryIndexEntryIterable.super.iterator();

	    @Override
	    public Key getStartRowKey() {
		return null;
	    }

	    @Override
	    public Key getEndRowKey() {
		return null;
	    }

	    @Override
	    public boolean hasNext() {
		return iterator.hasNext();
	    }

	    @Override
	    public SecondaryIndexEntry next() {
		return iterator.next();
	    }

	    @Override
	    public SecondaryIndexEntry peek() {
		return iterator.peek();
	    }

	    @Override
	    public void close() throws IOException {
		// TODO Auto-generated method stub

	    }

	};
    }

    public SecondaryIndexIterator iterator(Key startRowKey, Key stopRowKey) {
	return new SecondaryIndexIterator() {
	    private final PeekingIterator<SecondaryIndexEntry> iterator = SecondaryIndexEntryIterable.super.iterator();

	    @Override
	    public Key getStartRowKey() {
		return startRowKey;
	    }

	    @Override
	    public Key getEndRowKey() {
		return stopRowKey;
	    }

	    @Override
	    public boolean hasNext() {
		return iterator.hasNext();
	    }

	    @Override
	    public SecondaryIndexEntry next() {
		return iterator.next();
	    }

	    @Override
	    public SecondaryIndexEntry peek() {
		return iterator.peek();
	    }

	    @Override
	    public void close() throws IOException {
		// TODO Auto-generated method stub

	    }
	};
    }

}
