package com.puresoltechnologies.ductiledb.storage.engine.cf;

import java.util.TreeSet;

import com.puresoltechnologies.ductiledb.storage.engine.utils.ByteArrayComparator;

/**
 * This is a set of column identifiers. These are sorted in the order of
 * retrieval.
 * 
 * @author Rick-Rainer Ludwig
 */
public class ColumnKeySet extends TreeSet<byte[]> {

    private static final long serialVersionUID = 2126693320865897642L;

    public ColumnKeySet() {
	super(ByteArrayComparator.getInstance());
    }

}
