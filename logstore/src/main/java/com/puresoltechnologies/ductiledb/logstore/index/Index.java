package com.puresoltechnologies.ductiledb.logstore.index;

import java.io.File;

import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This method is used to provide an index.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface Index extends Iterable<IndexEntry> {

    /**
     * This method creates a new index.
     * 
     * @param storage
     *            is the {@link Storage} to be used to read and create the index.
     * @param directory
     *            is used to get all information for the column family to get
     *            indexed.
     * 
     * @return An {@link Index} is returned to be used by column families to find
     *         entries in database files.
     */
    public static Index create(Storage storage, File directory) {
	return new IndexImpl(storage, directory);
    }

    public static Index open(Storage storage, File directory, File metadataFile) {
	return new IndexImpl(storage, directory, metadataFile);
    }

    /**
     * This method is called to update the index by data file defined by the base
     * filename.
     */
    public void update();

    /**
     * This method is called to update the index by data file defined by the
     * provided filename.
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
     * This method is used to find a certain row in the index or better the range in
     * which the row can be found.
     * 
     * @param rowKey
     *            is the row to be looked after.
     * @return An {@link OffsetRange} is returned containing the result. It is a
     *         range in which the entry can be found. <code>null</code> is returned
     *         to indicate that the key is not available.
     */
    public OffsetRange find(Key rowKey);

    public IndexEntry ceiling(Key rowKey);

    public IndexEntry floor(Key rowKey);
}
