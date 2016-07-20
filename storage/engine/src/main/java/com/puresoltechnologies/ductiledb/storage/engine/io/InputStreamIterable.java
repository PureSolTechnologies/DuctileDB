package com.puresoltechnologies.ductiledb.storage.engine.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 * This abstract class is used to create an {@link Iterable} out of an
 * {@link InputStream}.
 * 
 * @author Rick-Rainer Ludwig
 *
 * @param <T>
 *            is the element type.
 */
public abstract class InputStreamIterable<T> implements ClosableIterable<T> {

    private final InputStream inputStream;

    public InputStreamIterable(InputStream inputStream) {
	super();
	this.inputStream = inputStream;
    }

    protected InputStream getInputStream() {
	return inputStream;
    }

    @Override
    public void close() throws IOException {
	inputStream.close();
    }

    @Override
    public Iterator<T> iterator() {
	return new Iterator<T>() {

	    private T nextEntry = null;

	    @Override
	    public boolean hasNext() {
		if (nextEntry != null) {
		    return true;
		}
		nextEntry = readEntry();
		return nextEntry != null;
	    }

	    @Override
	    public T next() {
		if (nextEntry == null) {
		    return readEntry();
		} else {
		    T result = nextEntry;
		    nextEntry = null;
		    return result;
		}
	    }
	};
    }

    protected abstract T readEntry();
}