package com.puresoltechnologies.ductiledb.storage.spi;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

/**
 * This is the central interface for the storage system to be used for
 * DuctileDB. This interface can be used to use plain file systems, Hadoop or
 * any other storage needed. com.puresoltechnologies.ductiledb.spi
 * 
 * @author Rick-Rainer Ludwig
 */
public interface Storage {

    /**
     * This method is used to check the preconditions and to initialize the
     * storage.
     * 
     * @throws IOException
     *             is thrown in case the initialization fails.
     */
    public void initialize() throws IOException;

    /**
     * This method creates a new directory.
     * 
     * @param storageName
     *            is the name of the directory.
     * @throws IOException
     */
    public void createDirectory(File storageName) throws IOException;

    /**
     * This method removes the directory
     * 
     * @param directory
     *            is the directory to be deleted.
     * @param recursive
     *            defines whether the directory is to be deleted recursively.
     * @throws IOException
     * @throws FileNotFoundException
     */
    public void removeDirectory(File directory, boolean recursive) throws FileNotFoundException, IOException;

    /**
     * This method lists the content of a folder.
     * 
     * @param directory
     * @return
     */
    public Iterator<File> list(File storageDirectory);

}
