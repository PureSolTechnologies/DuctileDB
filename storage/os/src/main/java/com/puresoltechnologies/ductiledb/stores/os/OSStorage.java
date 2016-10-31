package com.puresoltechnologies.ductiledb.stores.os;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.puresoltechnologies.ductiledb.storage.spi.CloseListener;
import com.puresoltechnologies.ductiledb.storage.spi.FileStatus;
import com.puresoltechnologies.ductiledb.storage.spi.FileType;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;
import com.puresoltechnologies.ductiledb.storage.spi.StorageConfiguration;

public class OSStorage implements Storage, CloseListener {

    public static final String DIRECTORY_PROPERTY = "storage.os.directory";
    public static final String DELETION_PERIOD_PROPERTY = "storage.os.deletion.period";
    private static final int DEFAULT_DELETION_PERIOD = 10000;

    private final ReentrantReadWriteLock deletionLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock deletionReadLock = deletionLock.readLock();
    private final ReentrantReadWriteLock.WriteLock deletionWriteLock = deletionLock.writeLock();

    private final Set<File> deletedFiles = new HashSet();
    private final WeakHashMap<Closeable, File> registeredStreams = new WeakHashMap<>();

    private final StorageConfiguration configuration;
    private final int blockSize;
    private final File rootDirectory;
    private final int deletionPeriod;

    private final Thread deletionThread;

    public OSStorage(StorageConfiguration configuration) {
	super();
	this.configuration = configuration;
	this.blockSize = configuration.getBlockSize();
	Properties properties = configuration.getProperties();
	String directory = (String) properties.get(DIRECTORY_PROPERTY);
	if (directory == null) {
	    throw new IllegalArgumentException("Directory was not set via property '" + DIRECTORY_PROPERTY + "'.");
	}
	rootDirectory = new File(directory);

	String deletionPeriodString = (String) properties.get(DELETION_PERIOD_PROPERTY);
	if (deletionPeriodString == null) {
	    this.deletionPeriod = DEFAULT_DELETION_PERIOD;
	} else {
	    this.deletionPeriod = Integer.parseInt(deletionPeriodString);
	}

	deletionThread = new Thread(new Runnable() {

	    @Override
	    public void run() {
		while (true) {
		    try {
			TimeUnit.MILLISECONDS.sleep(deletionPeriod);
			Iterator<File> iterator = deletedFiles.iterator();
			while (iterator.hasNext()) {
			    File file = iterator.next();
			    if (!registeredStreams.containsValue(file)) {
				if (performDeletion(file)) {
				    iterator.remove();
				}
			    }
			}
		    } catch (InterruptedException e) {
			return;
		    }
		}
	    }
	});
	deletionThread.start();
    }

    @Override
    public final StorageConfiguration getConfiguration() {
	return configuration;
    }

    @Override
    public File getStorageDirectory() {
	return rootDirectory;
    }

    @Override
    public void initialize() throws IOException {
	if (rootDirectory.exists()) {
	    if (!rootDirectory.isDirectory()) {
		throw new IOException("Could not create directory '" + rootDirectory.getPath()
			+ "', because there is a file with same name.");
	    }
	} else {
	    if (!rootDirectory.mkdirs()) {
		throw new IOException("Could not create directory '" + rootDirectory.getPath() + "'.");
	    }
	}
    }

    @Override
    public void close() throws IOException {
	deletionThread.interrupt();
    }

    @Override
    public void notifyClose(Closeable closeable) {
	File file = registeredStreams.remove(closeable);
	if (deletedFiles.contains(file)) {
	    if (!registeredStreams.containsValue(file)) {
		if (performDeletion(file)) {
		    deletedFiles.remove(file);
		}
	    }
	}
    }

    private boolean registerStream(File file, Closeable stream) {
	deletionReadLock.lock();
	try {
	    if (!deletedFiles.contains(file)) {
		registeredStreams.put(stream, file);
		return true;
	    } else {
		return false;
	    }
	} finally {
	    deletionReadLock.unlock();
	}
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
    public BufferedInputStream open(File file) throws IOException {
	BufferedInputStream storageInputStream = new BufferedInputStream(
		new FileInputStream(new File(rootDirectory, file.getPath())), blockSize) {
	    @Override
	    protected void finalize() throws Throwable {
		close();
		super.finalize();
	    }

	    @Override
	    public void close() throws IOException {
		notifyClose(this);
		super.close();
	    }
	};
	if (registerStream(file, storageInputStream)) {
	    return storageInputStream;
	} else {
	    storageInputStream.close();
	    throw new IOException("File '" + file + "' was already deleted.");
	}
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
	BufferedOutputStream storageOutputStream = new BufferedOutputStream(new FileOutputStream(path), blockSize) {

	    @Override
	    protected void finalize() throws Throwable {
		close();
		super.finalize();
	    }

	    @Override
	    public void close() throws IOException {
		notifyClose(this);
		super.close();
	    }

	};
	if (registerStream(file, storageOutputStream)) {
	    return storageOutputStream;
	} else {
	    storageOutputStream.close();
	    throw new IOException("File '" + file + "' was already deleted.");
	}
    }

    @Override
    public void delete(File file) {
	deletionWriteLock.lock();
	if (!registeredStreams.containsValue(file)) {
	    performDeletion(file);
	} else {
	    deletedFiles.add(file);
	}
	deletionWriteLock.unlock();
    }

    private boolean performDeletion(File file) {
	return new File(rootDirectory, file.getPath()).delete();
    }

    @Override
    public BufferedOutputStream append(File file) throws IOException {
	File path = new File(rootDirectory, file.getPath());
	if (!path.exists()) {
	    throw new IOException("File '" + file + "' does not exist.");
	}
	BufferedOutputStream storageOutputStream = new BufferedOutputStream(new FileOutputStream(path, true),
		blockSize) {
	    @Override
	    protected void finalize() throws Throwable {
		close();
		super.finalize();
	    }

	    @Override
	    public void close() throws IOException {
		notifyClose(this);
		super.close();
	    }
	};
	if (registerStream(file, storageOutputStream)) {
	    return storageOutputStream;
	} else {
	    storageOutputStream.close();
	    throw new IOException("File '" + file + "' was already deleted.");
	}
    }

    @Override
    public String toString() {
	return rootDirectory.getPath();
    }
}
