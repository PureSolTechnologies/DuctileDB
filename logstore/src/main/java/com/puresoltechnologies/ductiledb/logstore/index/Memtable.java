package com.puresoltechnologies.ductiledb.logstore.index;

import java.io.IOException;

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

    private final RedBlackTree<Key, IndexEntry> values = new RedBlackTree<>();

    public Memtable() {
	super();
    }

    public void clear() {
	values.clear();
    }

    public int size() {
	return values.size();
    }

    public void put(IndexEntry indexEntry) {
	values.put(indexEntry.getRowKey(), indexEntry);
    }

    public IndexEntry get(Key rowKey) {
	return values.get(rowKey);
    }

    public void delete(Key rowKey) {
	values.delete(rowKey);
    }

    @Override
    public IndexIterator iterator() {
	return new IndexIterator() {

	    private final StreamIterator<RedBlackTreeNode<Key, IndexEntry>> iterator = values.iterator();

	    @Override
	    public boolean hasNext() {
		return iterator.hasNext();
	    }

	    @Override
	    public IndexEntry next() {
		return iterator.next().getValue();
	    }

	    @Override
	    public IndexEntry peek() {
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

    public IndexIterator iterator(Key startKey, Key endKey) {
	return new IndexIterator() {

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
	    public boolean hasNext() {
		return iterator.hasNext();
	    }

	    @Override
	    public IndexEntry next() {
		return iterator.next().getValue();
	    }

	    @Override
	    public IndexEntry peek() {
		return iterator.peek().getValue();
	    }

	    @Override
	    public void close() throws IOException {
		// intentionally left empty
	    }

	};
    }
}
