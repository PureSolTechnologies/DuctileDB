package com.puresoltechnologies.ductiledb.blobstore;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

import com.puresoltechnologies.commons.misc.hash.HashId;

/**
 * This is the central interface for the BLOB store in DuctileDB.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface BlobStore {

    /**
     * Returns the number of blobs in the store.
     * 
     * @return
     * @throws SQLException
     */
    public long getBlobCount() throws SQLException;

    /**
     * Returns the number of bytes in the stored in total in the store.
     * 
     * @return
     * @throws SQLException
     */
    public long getBlobStoreSize() throws SQLException;

    /**
     * Check whether a BLOB object is available identified by its hash id.
     * 
     * @param hashId
     *            is the {@link HashId} of the object to be checked for
     *            existence.
     * @return <code>true</code> is returned in case the object is available for
     *         reading.
     * @throws SQLException
     * @throws IOException
     *             is thrown in case of Hadoop I/O issues.
     */
    public boolean isBlobAvailable(HashId hashId) throws SQLException;

    /**
     * Returns the size of a BLOB object identified by its hash id.
     * 
     * @param hashId
     *            is the {@link HashId} of the object to get the size of.
     * @return The size is returned as long.
     * @throws SQLException
     * @throws FileNotFoundException
     *             is thrown in case the referenced object does not exist.
     * @throws IOException
     *             is thrown in case of Hadoop I/O issues.
     */
    public long getBlobSize(HashId hashId) throws SQLException;

    /**
     * This method reads a BLOB object from the store.
     * 
     * @param hashId
     *            is the {@link HashId} reference for the BLOB object.
     * @return An {@link InputStream} is returned.
     * @throws FileNotFoundException
     *             is thrown in case the referenced object is not present.
     * @throws IOException
     *             is thrown in case of Hadoop I/O issues.
     */
    public InputStream readBlob(HashId hashId);

    /**
     * This method writes a new BLOB object to the store.
     * 
     * @return A {@link HashId} reference is returned to be used for the BLOB
     *         object. The has is created out of the content of the object.
     * @throws SQLException
     * @throws IOException
     *             is IO issues.
     */
    public HashId storeBlob(InputStream inputStream) throws SQLException, IOException;

    /**
     * This method writes a new BLOB object to the store.
     * 
     * @param hashId
     *            is the {@link HashId} reference used for the BLOB object. it
     *            is best to create the {@link HashId} out of the content of the
     *            object.
     * @throws SQLException
     * @throws IOException
     *             is thrown in case of Hadoop I/O issues.
     */
    public void storeBlob(HashId hashId, InputStream inputStream) throws SQLException, IOException;

    /**
     * This method removes a BLOB object from the store.
     * 
     * @param hashId
     *            is the {@link HashId} reference of the BLOB object to be
     *            deleted.
     * @throws SQLException
     * @throws IOException
     *             is thrown in case of Hadoop I/O issues.
     */
    public boolean removeBlob(HashId hashId) throws SQLException;

}
