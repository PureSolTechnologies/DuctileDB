package com.puresoltechnologies.ductiledb.logstore.index;

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
