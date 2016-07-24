package com.puresoltechnologies.ductiledb.storage.spi;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;

/**
 * This is the central interface for the storage system to be used for
 * DuctileDB. This interface can be used to use plain file systems, Hadoop or
 * any other storage needed. com.puresoltechnologies.ductiledb.spi
 * 
 * <b>The implemenation of this storage needs to be stateless and therefore
 * thread-safe.</b>
 * 
 * @author Rick-Rainer Ludwig
 */
public interface Storage extends Closeable {

    /**
     * This method returns the confituration of the storage device.
     * 
     * @return A {@link StorageConfiguration} object is returned.
     */
    public StorageConfiguration getConfiguration();

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
    public Iterable<File> list(File directory);

    /**
     * This method lists the content of a folder.
     * 
     * @param directory
     * @param filter
     * @return
     */
    public Iterable<File> list(File directory, FilenameFilter filter);

    public boolean exists(File file);

    public boolean isDirectory(File directory);

    public FileStatus getFileStatus(File file);

    public BufferedInputStream open(File file) throws FileNotFoundException;

    public BufferedOutputStream create(File file) throws IOException;

    public BufferedOutputStream append(File file) throws IOException;

    public boolean delete(File file);

}
