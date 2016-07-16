package com.puresoltechnologies.ductiledb.stores.os;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
	if (!rootDirectory.mkdirs()) {
	    throw new IOException("Could not create directory '" + rootDirectory.getPath() + "'.");
	}
    }

    @Override
    public Iterator<File> list(File directory) {
	File[] files = new File(rootDirectory, directory.getPath()).listFiles();
	List<File> list = new ArrayList<>();
	for (File file : files) {
	    String directoryString = file.getPath().replace(rootDirectory.getPath(), "");
	    if (directoryString.startsWith(File.separator)) {
		directoryString = directoryString.substring(1);
	    }
	    list.add(new File(directoryString));
	}
	return list.iterator();
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
}
