package com.puresoltechnologies.ductiledb.engine.cf.index.secondary;

import java.io.IOException;

import com.puresoltechnologies.commons.misc.PeekingIterator;
import com.puresoltechnologies.ductiledb.engine.Key;
import com.puresoltechnologies.ductiledb.engine.cf.index.secondary.io.SecondaryIndexEntry;
import com.puresoltechnologies.ductiledb.engine.cf.index.secondary.io.SecondaryIndexIterator;
import com.puresoltechnologies.trees.RedBlackTree;
import com.puresoltechnologies.trees.RedBlackTreeNode;

public class SecondaryIndexMemtable implements Iterable<SecondaryIndexEntry> {

    private final RedBlackTree<Key, SecondaryIndexEntry> values = new RedBlackTree<>();

    public SecondaryIndexMemtable() {
	super();
    }

    public void clear() {
	values.clear();
    }

    public int size() {
	return values.size();
    }

    public void put(SecondaryIndexEntry indexEntry) {
	values.put(indexEntry.getRowKey(), indexEntry);
    }

    public SecondaryIndexEntry get(Key rowKey) {
	return values.get(rowKey);
    }

    public void delete(Key rowKey) {
	values.delete(rowKey);
    }

    @Override
    public SecondaryIndexIterator iterator() {
	return new SecondaryIndexIterator() {

	    private final PeekingIterator<RedBlackTreeNode<Key, SecondaryIndexEntry>> iterator = values.iterator();

	    @Override
	    public boolean hasNext() {
		return iterator.hasNext();
	    }

	    @Override
	    public SecondaryIndexEntry next() {
		return iterator.next().getValue();
	    }

	    @Override
	    public SecondaryIndexEntry peek() {
		return iterator.peek().getValue();
	    }

	    @Override
	    public void close() throws IOException {
		// intentionally left empty
	    }

	    @Override
	    public Key getStartRowKey() {
		return null;
	    }

	    @Override
	    public Key getEndRowKey() {
		return null;
	    }

	};
    }

    public SecondaryIndexIterator iterator(Key startKey, Key endKey) {
	return new SecondaryIndexIterator() {

	    private final PeekingIterator<RedBlackTreeNode<Key, SecondaryIndexEntry>> iterator = values
		    .iterator(startKey, endKey);

	    @Override
	    public Key getStartRowKey() {
		return startKey;
	    }

	    @Override
	    public Key getEndRowKey() {
		return endKey;
	    }

	    @Override
	    public boolean hasNext() {
		return iterator.hasNext();
	    }

	    @Override
	    public SecondaryIndexEntry next() {
		return iterator.next().getValue();
	    }

	    @Override
	    public SecondaryIndexEntry peek() {
		return iterator.peek().getValue();
	    }

	    @Override
	    public void close() throws IOException {
		// intentionally left empty
	    }

	};
    }

}
