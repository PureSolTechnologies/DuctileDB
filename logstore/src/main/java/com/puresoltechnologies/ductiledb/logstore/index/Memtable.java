package com.puresoltechnologies.ductiledb.logstore.index;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.streaming.StreamIterator;
import com.puresoltechnologies.trees.RedBlackTree;
import com.puresoltechnologies.trees.RedBlackTreeNode;

/**
 * Basic Memtable implementation.
 * 
 * @author Rick-Rainer Ludwig
 */
public class Memtable implements Iterable<IndexEntry> {

    private final ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock(true);
    private final ReadLock readLock = reentrantReadWriteLock.readLock();
    private final WriteLock writeLock = reentrantReadWriteLock.writeLock();

    private final RedBlackTree<Key, IndexEntry> values = new RedBlackTree<>();

    public Memtable() {
	super();
    }

    public void clear() {
	writeLock.lock();
	try {
	    values.clear();
	} finally {
	    writeLock.unlock();
	}
    }

    public int size() {
	readLock.lock();
	try {
	    return values.size();
	} finally {
	    readLock.unlock();
	}
    }

    public void put(IndexEntry indexEntry) {
	writeLock.lock();
	try {
	    values.put(indexEntry.getRowKey(), indexEntry);
	} finally {
	    writeLock.unlock();
	}
    }

    public IndexEntry get(Key rowKey) {
	readLock.lock();
	try {
	    return values.get(rowKey);
	} finally {
	    readLock.unlock();
	}
    }

    public void delete(Key rowKey) {
	writeLock.lock();
	try {
	    values.delete(rowKey);
	} finally {
	    writeLock.unlock();
	}
    }

    @Override
    public IndexEntryIterator iterator() {
	return new IndexEntryIterator() {

	    private final StreamIterator<RedBlackTreeNode<Key, IndexEntry>> iterator = values.iterator();

	    @Override
	    public Key getStartRowKey() {
		return null;
	    }

	    @Override
	    public Key getEndRowKey() {
		return null;
	    }

	    @Override
	    protected IndexEntry findNext() {
		return iterator.hasNext() ? iterator.next().getValue() : null;

	    }

	};
    }

    public IndexEntryIterator iterator(Key startKey, Key endKey) {
	return new IndexEntryIterator() {

	    private final StreamIterator<RedBlackTreeNode<Key, IndexEntry>> iterator = values.iterator(startKey,
		    endKey);

	    @Override
	    public Key getStartRowKey() {
		return startKey;
	    }

	    @Override
	    public Key getEndRowKey() {
		return endKey;
	    }

	    @Override
	    protected IndexEntry findNext() {
		return iterator.hasNext() ? iterator.next().getValue() : null;
	    }

	};
    }
}
