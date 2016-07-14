package com.puresoltechnologies.ductiledb.stores.os;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class OSStorage implements Storage {

    public static final String DIRECTORY_PROPERTY = "storage.os.directory";

    private final File rootDirectory;

    public OSStorage(Map<String, String> configuration) {
	String directory = configuration.get(DIRECTORY_PROPERTY);
	if (directory == null) {
	    throw new IllegalArgumentException("Directory was not set via property '" + DIRECTORY_PROPERTY + "'.");
	}
	rootDirectory = new File(directory);
    }

    public final File getRootDirectory() {
	return rootDirectory;
    }

    @Override
    public void initialize() throws IOException {
	createDirectory(rootDirectory);
    }

    private void createDirectory(File directory) throws IOException {
	if (!directory.exists()) {
	    if (!directory.mkdirs()) {
		throw new IOException("Could not create directory '" + directory + "'.");
	    }
	}
    }

    @Override
    public void createDirectory(String storageName) throws IOException {
	createDirectory(new File(rootDirectory, storageName));
    }

    @Override
    public void removeDirectory(String directory, boolean recursive) throws FileNotFoundException, IOException {
	File dir = new File(rootDirectory, directory);
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

}
