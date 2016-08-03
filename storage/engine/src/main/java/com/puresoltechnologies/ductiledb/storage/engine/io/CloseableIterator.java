package com.puresoltechnologies.ductiledb.storage.engine.io;

import java.io.Closeable;
import java.util.Iterator;

/**
 * This interface is used to combine an interator with {@link Closable} to have
 * an iterator over a closable resource like files.
 * 
 * @author Rick-Rainer Ludwig
 *
 * @param <T>
 */
public interface CloseableIterator<T> extends Iterator<T>, Closeable {
}
