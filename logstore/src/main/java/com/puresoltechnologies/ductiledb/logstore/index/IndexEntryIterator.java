package com.puresoltechnologies.ductiledb.logstore.index;

import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.streaming.AbstractStreamIterator;

/**
 * This interface is used for intex iterators which provide special features for
 * indizes besides the normal iterator.
 * 
 * @author Rick-Rainer Ludwig
 */
public abstract class IndexEntryIterator extends AbstractStreamIterator<IndexEntry> {

    /**
     * Returns the start row key.
     * 
     * @return
     */
    public abstract Key getStartRowKey();

    /**
     * Returns the end row key.
     * 
     * @return
     */
    public abstract Key getEndRowKey();

    /**
     * This method moves the iterator to the element before the given element or the
     * first element which has a greater row.
     * 
     * @param rowKey
     */
    public final void gotoStart(Key startRowKey) {
	if (startRowKey != null) {
	    while ((hasNext()) && (peek().getRowKey().compareTo(startRowKey) < 0)) {
		next();
	    }
	}
    }

    public final void skip() {
	next();
    }
}
