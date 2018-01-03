package com.puresoltechnologies.ductiledb.storage.spi;

/**
 * A simple interface to mark storage streams.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface StorageStream {

    /**
     * Returns the current position within the stream.
     * 
     * @return A long is returned providing the position.
     */
    public long getPosition();

}
