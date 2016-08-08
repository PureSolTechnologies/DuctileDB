package com.puresoltechnologies.ductiledb.storage.engine.index;

import java.io.IOException;

import com.puresoltechnologies.commons.misc.PeekingIterator;
import com.puresoltechnologies.trees.RedBlackTree;
import com.puresoltechnologies.trees.RedBlackTreeNode;

/**
 * Basic Memtable implementation.
 * 
 * @author Rick-Rainer Ludwig
 */
public class Memtable implements Iterable<IndexEntry> {

    private final RedBlackTree<RowKey, IndexEntry> values = new RedBlackTree<>();

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

    public IndexEntry get(RowKey rowKey) {
	return values.get(rowKey);
    }

    public void delete(RowKey rowKey) {
	values.delete(rowKey);
    }

    @Override
    public IndexIterator iterator() {
	return new IndexIterator() {

	    private final PeekingIterator<RedBlackTreeNode<RowKey, IndexEntry>> iterator = values.iterator();

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
	    public RowKey getStartRowKey() {
		return null;
	    }

	    @Override
	    public RowKey getEndRowKey() {
		return null;
	    }

	};
    }

    public IndexIterator iterator(RowKey startKey, RowKey endKey) {
	return new IndexIterator() {

	    private final PeekingIterator<RedBlackTreeNode<RowKey, IndexEntry>> iterator = values.iterator(startKey,
		    endKey);

	    @Override
	    public RowKey getStartRowKey() {
		return startKey;
	    }

	    @Override
	    public RowKey getEndRowKey() {
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
