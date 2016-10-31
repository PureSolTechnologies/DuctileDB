package com.puresoltechnologies.ductiledb.storage.spi;

import java.io.Closeable;

/**
 * This interface is used to notify the implementer of a closed
 * {@link Closeable}.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface CloseListener {

    public void notifyClose(Closeable inputStream);

}
