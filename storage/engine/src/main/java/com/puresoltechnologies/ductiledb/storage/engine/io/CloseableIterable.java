package com.puresoltechnologies.ductiledb.storage.engine.io;

import java.io.Closeable;

/**
 * This iterable is used on closable sources lik I/O streams. It is used to
 * iterate of a source and then to close it afterwards.
 * 
 * @author Rick-Rainer Ludwig
 *
 * @param <T>
 *            is type of the iterating variable.
 */
public interface CloseableIterable<T> extends Closeable, Iterable<T> {
}
