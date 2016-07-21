package com.puresoltechnologies.ductiledb.stores.hadoop;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;

import com.puresoltechnologies.ductiledb.storage.spi.FileStatus;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;
import com.puresoltechnologies.ductiledb.storage.spi.StorageConfiguration;

/**
 * This is a DuctileDB storage for Hadoop.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class HadoopStorage implements Storage {

    private final FileSystem fileSystem;

    public HadoopStorage(StorageConfiguration configuration) {
	super();
	this.fileSystem = null;
    }

    public void createFile() {
	// TODO
    }

    @Override
    public void initialize() throws IOException {
	// TODO Auto-generated method stub

    }

    @Override
    public void close() throws IOException {
	fileSystem.close();
    }

    @Override
    public void createDirectory(File storageName) throws IOException {
	// TODO Auto-generated method stub
	// fileSystem.create(new PathUI)
    }

    @Override
    public void removeDirectory(File directory, boolean recursive) throws FileNotFoundException, IOException {
	// TODO Auto-generated method stub
	// fileSystem.delete(f, recursive)
	// fileSystem.list()
    }

    @Override
    public Iterable<File> list(File storageDirectory) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public Iterable<File> list(File storageDirectory, FilenameFilter filter) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public boolean exists(File file) {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean isDirectory(File directory) {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public FileStatus getFileStatus(File file) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public InputStream open(File file) throws FileNotFoundException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public OutputStream create(File file) throws IOException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public boolean delete(File file) {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public OutputStream append(File file) throws IOException {
	// TODO Auto-generated method stub
	return null;
    }

}
