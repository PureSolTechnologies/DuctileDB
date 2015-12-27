package com.puresoltechnologies.ductiledb.core.utils;

import java.util.Iterator;

/**
 * A simple implementation of an empty {@link Iterator} to signal empty results.
 * 
 * @author Rick-Rainer Ludwig
 *
 * @param <T>
 */
public class EmptyIterator<T> implements Iterator<T> {

    @Override
    public boolean hasNext() {
	return false;
    }

    @Override
    public T next() {
	return null;
    }
}
