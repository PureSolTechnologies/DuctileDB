package com.puresoltechnologies.ductiledb.stores.os;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.puresoltechnologies.ductiledb.storage.spi.FileStatus;
import com.puresoltechnologies.ductiledb.storage.spi.FileType;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;
import com.puresoltechnologies.ductiledb.storage.spi.StorageConfiguration;

public class OSStorage implements Storage {

    public static final String DIRECTORY_PROPERTY = "storage.os.directory";

    private final StorageConfiguration configuration;
    private final int blockSize;
    private final File rootDirectory;

    public OSStorage(StorageConfiguration configuration) {
	super();
	this.configuration = configuration;
	this.blockSize = configuration.getBlockSize();
	String directory = (String) configuration.getProperties().get(DIRECTORY_PROPERTY);
	if (directory == null) {
	    throw new IllegalArgumentException("Directory was not set via property '" + DIRECTORY_PROPERTY + "'.");
	}
	rootDirectory = new File(directory);
    }

    @Override
    public final StorageConfiguration getConfiguration() {
	return configuration;
    }

    public final File getRootDirectory() {
	return rootDirectory;
    }

    @Override
    public void initialize() throws IOException {
	if (!rootDirectory.mkdirs()) {
	    throw new IOException("Could not create directory '" + rootDirectory.getPath() + "'.");
	}
    }

    @Override
    public void close() throws IOException {
	// intentionally left empty
    }

    @Override
    public Iterable<File> list(File directory) {
	File[] files = new File(rootDirectory, directory.getPath()).listFiles();
	List<File> list = new ArrayList<>();
	if (files == null) {
	    return list;
	}
	for (File file : files) {
	    String directoryString = file.getPath().replace(rootDirectory.getPath(), "");
	    if (directoryString.startsWith(File.separator)) {
		directoryString = directoryString.substring(1);
	    }
	    list.add(new File(directoryString));
	}
	return list;
    }

    @Override
    public Iterable<File> list(File directory, FilenameFilter filter) {
	File[] files = new File(rootDirectory, directory.getPath()).listFiles(new FilenameFilter() {

	    @Override
	    public boolean accept(File directory, String name) {
		return filter.accept(new File(directory.getPath().replace(rootDirectory.getPath(), "")), name);
	    }
	});
	List<File> list = new ArrayList<>();
	if (files != null) {
	    for (File file : files) {
		String directoryString = file.getPath().replace(rootDirectory.getPath(), "");
		if (directoryString.startsWith(File.separator)) {
		    directoryString = directoryString.substring(1);
		}
		list.add(new File(directoryString));
	    }
	}
	return list;
    }

    @Override
    public void createDirectory(File directory) throws IOException {
	File dir = new File(rootDirectory, directory.getPath());
	if (!dir.exists()) {
	    if (!dir.mkdirs()) {
		throw new IOException("Could not create directory '" + dir + "'.");
	    }
	}
    }

    @Override
    public void removeDirectory(File directory, boolean recursive) throws FileNotFoundException, IOException {
	File dir = new File(rootDirectory, directory.getPath());
	if (!dir.exists()) {
	    throw new FileNotFoundException("Directory '" + directory + "' does not exist.");
	}
	if (!dir.isDirectory()) {
	    throw new IOException("Directory '" + directory + "' is not a directory.");
	}
	if (recursive) {
	    removeRecursively(dir);
	} else {
	    if (!dir.delete()) {
		new IOException("Directory '" + directory + "' could not deleted.");
	    }
	}

    }

    private void removeRecursively(File dir) {
	if (dir.isDirectory()) {
	    for (File file : dir.listFiles()) {
		removeRecursively(file);
	    }
	}
	if (!dir.delete()) {
	    new IOException(
		    "Directory '" + dir.getPath().replace(rootDirectory.getPath(), "") + "' could not deleted.");
	}
    }

    @Override
    public boolean exists(File file) {
	return new File(rootDirectory, file.getPath()).exists();
    }

    @Override
    public boolean isDirectory(File directory) {
	return new File(rootDirectory, directory.getPath()).isDirectory();
    }

    @Override
    public FileStatus getFileStatus(File file) {
	File path = new File(rootDirectory, file.getPath());
	if (!path.exists()) {
	    return null;
	}
	FileType fileType;
	if (path.isDirectory()) {
	    fileType = FileType.DIRECTORY;
	} else if (path.isFile()) {
	    fileType = FileType.FILE;
	} else {
	    fileType = FileType.UNKNOWN;
	}
	return new FileStatus(file, fileType, path.isHidden(), path.length());
    }

    @Override
    public BufferedInputStream open(File file) throws FileNotFoundException {
	return new BufferedInputStream(new FileInputStream(new File(rootDirectory, file.getPath())), blockSize);
    }

    @Override
    public BufferedOutputStream create(File file) throws IOException {
	File path = new File(rootDirectory, file.getPath());
	if (path.exists()) {
	    throw new IOException("File '" + file + "' exists already.");
	}
	if (!path.createNewFile()) {
	    throw new IOException("File '" + file + "' could not be created.");
	}
	return new BufferedOutputStream(new FileOutputStream(path), blockSize);
    }

    @Override
    public boolean delete(File file) {
	return new File(rootDirectory, file.getPath()).delete();
    }

    @Override
    public BufferedOutputStream append(File file) throws IOException {
	File path = new File(rootDirectory, file.getPath());
	if (!path.exists()) {
	    throw new IOException("File '" + file + "' does not exist.");
	}
	return new BufferedOutputStream(new FileOutputStream(path, true), blockSize);
    }

    @Override
    public String toString() {
	return rootDirectory.getPath();
    }
}
