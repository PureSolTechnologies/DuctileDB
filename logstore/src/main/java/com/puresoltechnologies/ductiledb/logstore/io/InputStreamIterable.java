package com.puresoltechnologies.ductiledb.logstore.io;

import java.io.IOException;
import java.io.InputStream;

import com.puresoltechnologies.commons.misc.io.CloseableIterable;
import com.puresoltechnologies.streaming.StreamIterator;
import com.puresoltechnologies.streaming.streams.InputStreamIterator;

/**
 * This abstract class is used to create an {@link Iterable} out of an
 * {@link InputStream}.
 * 
 * <b>This class is not thread-safe!</b>
 * 
 * @author Rick-Rainer Ludwig
 *
 * @param <T>
 *            is the element type.
 */
public abstract class InputStreamIterable<I extends InputStream, T> implements CloseableIterable<T> {

    private final I inputStream;

    public InputStreamIterable(I inputStream) {
	super();
	this.inputStream = inputStream;
    }

    protected final I getInputStream() {
	return inputStream;
    }

    @Override
    public final void close() throws IOException {
	inputStream.close();
    }

    @Override
    public StreamIterator<T> iterator() {
	return new InputStreamIterator<>(inputStream, i -> readEntry(i));
    }

    protected abstract T readEntry(I inputStream);

}