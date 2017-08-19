package com.puresoltechnologies.ductiledb.logstore.index;

import java.io.Closeable;

import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.streaming.StreamIterator;

/**
 * This interface is used for intex iterators which provide special features for
 * indizes besides the normal iterator.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface IndexIterator extends StreamIterator<IndexEntry>, Closeable {

    /**
     * Returns the start row key.
     * 
     * @return
     */
    public Key getStartRowKey();

    /**
     * Returns the end row key.
     * 
     * @return
     */
    public Key getEndRowKey();

    /**
     * This method moves the iterator to the element before the given element or the
     * first element which has a greater row.
     * 
     * @param rowKey
     */
    default public void gotoStart(Key startRowKey) {
	if (startRowKey != null) {
	    while ((hasNext()) && (peek().getRowKey().compareTo(startRowKey) < 0)) {
		next();
	    }
	}
    }

    default public void skip() {
	next();
    }
}
