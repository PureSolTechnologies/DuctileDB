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
     * This method is used to register a new index entry.
     * 
     * @param rowKey
     * @param indexEntry
     */
    public void put(byte[] rowKey, IndexEntry indexEntry);

    /**
     * This method returns the value for the given row key.
     * 
     * @param rowKey
     * @return
     */
    public IndexEntry get(byte[] rowKey);

    /**
     * This method is used to find a certain row in the index or better the
     * range in which the row can be found.
     * 
     * @param rowKey
     *            is the row to be looked after.
     * @return An {@link OffsetRange} is returned containing the result. It is a
     *         range in which the entry can be found. <code>null</code> is
     *         returned to indicate that the key is not available.
     */
    public OffsetRange find(byte[] rowKey);

}
