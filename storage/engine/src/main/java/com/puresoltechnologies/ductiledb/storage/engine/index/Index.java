package com.puresoltechnologies.ductiledb.storage.engine.index;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;

/**
 * This method is used to provide an in-memory index.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface Index {

    /**
     * This method is called to update the index by data filed defined by the
     * base filename.
     * 
     * @throws StorageException
     */
    public void update() throws StorageException;

    /**
     * This method is used to find a certain row in the index.
     * 
     * @param rowKey
     *            is the row to be looked after.
     * @return An {@link IndexEntry} is returned containing the result.
     *         <code>null</code> is returned to
     */
    public IndexEntry find(byte[] rowKey);

}
