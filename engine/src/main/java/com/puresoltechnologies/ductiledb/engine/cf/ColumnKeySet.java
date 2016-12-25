package com.puresoltechnologies.ductiledb.engine.cf;

import java.util.LinkedHashSet;
import java.util.Set;

import com.puresoltechnologies.ductiledb.logstore.Key;

/**
 * This is a set of column identifiers. These are sorted in the order of
 * retrieval.
 * 
 * @author Rick-Rainer Ludwig
 */
public class ColumnKeySet extends LinkedHashSet<Key> {

    private static final long serialVersionUID = 2126693320865897642L;

    public ColumnKeySet() {
	super();
    }

    public ColumnKeySet(Key... columns) {
	this();
	for (Key column : columns) {
	    add(column);
	}
    }

    public ColumnKeySet(Set<Key> columns) {
	this();
	addAll(columns);
    }
}
