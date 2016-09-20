package com.puresoltechnologies.ductiledb.storage.engine.cf.index.primary;

import java.io.File;

import com.puresoltechnologies.ductiledb.storage.engine.Key;

/**
 * This method is used to provide an in-memory index.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface Index extends Iterable<IndexEntry> {

    /**
     * This method is called to update the index by data filed defined by the
     * base filename.
     */
    public void update();

    /**
     * This method is called to update the index by data filed defined by the
     * base filename.
     */
    public void update(File latestMetadataFile);

    /**
     * This method is used to register a new index entry.
     * 
     * @param indexEntry
     */
    public void put(IndexEntry indexEntry);

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
    public OffsetRange find(Key rowKey);

    public IndexEntry ceiling(Key rowKey);

    public IndexEntry floor(Key rowKey);
}
