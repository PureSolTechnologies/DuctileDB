package com.puresoltechnologies.ductiledb.engine.cf.index.secondary.io;

import com.puresoltechnologies.commons.misc.PeekingIterator;
import com.puresoltechnologies.commons.misc.io.CloseableIterator;
import com.puresoltechnologies.ductiledb.engine.Key;

/**
 * This interface is used for intex iterators which provide special features for
 * indizes besides the normal iterator.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface SecondaryIndexIterator
	extends PeekingIterator<SecondaryIndexEntry>, CloseableIterator<SecondaryIndexEntry> {

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
     * This method moves the iterator to the element before the given element or
     * the first element which has a greater row.
     * 
     * @param rowKey
     */
    default public void gotoStart(Key startRowKey) {
	if (startRowKey != null) {
	    while ((hasNext()) && (peek().getRowKey().compareTo(startRowKey) < 0)) {
		skip();
	    }
	}
    }
}
