package com.puresoltechnologies.ductiledb.storage.spi;

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
     * This method returns the configuration of the storage device.
     * 
     * @return A {@link StorageConfiguration} object is returned.
     */
    public StorageConfiguration getConfiguration();

    /**
     * 
     * @return
     */
    public File getStorageDirectory();

    /**
     * This method is used to check the preconditions and to initialize the storage.
     * 
     * @throws IOException
     *             is thrown in case the initialization fails.
     */
    public void initialize() throws IOException;

    /**
     * This method creates a new directory.
     * 
     * @param directory
     *            is the name of the directory.
     * @throws IOException
     */
    public void createDirectory(File directory) throws IOException;

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

    public StorageInputStream open(File file) throws IOException;

    public StorageOutputStream create(File file) throws IOException;

    public StorageOutputStream append(File file) throws IOException;

    /**
     * This method deletes the specified file.
     * 
     * @param file
     * @return
     */
    public void delete(File file);

}
