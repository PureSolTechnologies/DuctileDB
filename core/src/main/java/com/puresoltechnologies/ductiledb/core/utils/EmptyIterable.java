package com.puresoltechnologies.ductiledb.core.utils;

import java.util.Iterator;

/**
 * A simple implementation of an empty {@link Iterable} to signal empty results.
 * 
 * @author Rick-Rainer Ludwig
 *
 *
 * @param <T>
 *            is the type of the variables in iterable.
 */
public class EmptyIterable<T> implements Iterable<T> {

    @Override
    public Iterator<T> iterator() {
	return new EmptyIterator<>();
    }

}
