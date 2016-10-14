package com.puresoltechnologies.ductiledb.core.blob;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import com.puresoltechnologies.commons.misc.hash.HashId;

/**
 * This is the central interface for the BLOB store in DuctileDB.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface BlobStore extends Closeable {

    public static String NAMESPACE = "blobs";

    /**
     * Check whether a BLOB object is available identified by its hash id.
     * 
     * @param hashId
     *            is the {@link HashId} of the object to be checked for
     *            existence.
     * @return <code>true</code> is returned in case the object is available for
     *         reading.
     * @throws IOException
     *             is thrown in case of Hadoop I/O issues.
     */
    public boolean isBlobAvailable(HashId hashId) throws IOException;

    /**
     * Returns the size of a BLOB object identified by its hash id.
     * 
     * @param hashId
     *            is the {@link HashId} of the object to get the size of.
     * @return The size is returned as long.
     * @throws FileNotFoundException
     *             is thrown in case the referenced object does not exist.
     * @throws IOException
     *             is thrown in case of Hadoop I/O issues.
     */
    public long getBlobSize(HashId hashId) throws FileNotFoundException, IOException;

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
    public InputStream readBlob(HashId hashId) throws FileNotFoundException, IOException;

    /**
     * This method writes a new BLOB object to the store.
     * 
     * @param hashId
     *            is the {@link HashId} reference used for the BLOB object. it
     *            is best to create the {@link HashId} out of the content of the
     *            object.
     * @throws IOException
     *             is thrown in case of Hadoop I/O issues.
     */
    public void storeBlob(HashId hashId, InputStream inputStream) throws IOException;

    /**
     * This method removes a BLOB object from the store.
     * 
     * @param hashId
     *            is the {@link HashId} reference of the BLOB object to be
     *            deleted.
     * @throws IOException
     *             is thrown in case of Hadoop I/O issues.
     */
    public boolean removeBlob(HashId hashId) throws IOException;

}
