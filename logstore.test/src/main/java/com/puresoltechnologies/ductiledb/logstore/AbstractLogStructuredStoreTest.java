package com.puresoltechnologies.ductiledb.logstore;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.puresoltechnologies.ductiledb.logstore.io.CurrentCommitLogFilenameFilter;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.api.StorageFactory;
import com.puresoltechnologies.ductiledb.storage.api.StorageFactoryServiceException;
import com.puresoltechnologies.ductiledb.storage.spi.FileStatus;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;
import com.puresoltechnologies.ductiledb.storage.spi.StorageConfiguration;

public abstract class AbstractLogStructuredStoreTest {

    private static Storage storage = null;
    private static LogStructuredStore store = null;

    @BeforeClass
    public static void readConfiguration() throws IOException, StorageFactoryServiceException, StorageException {
	StorageConfiguration configuration = new StorageConfiguration();
	configuration.setBlockSize(8192);
	Properties properties = new Properties();
	properties.put("storage.os.directory", "target/test");
	configuration.setProperties(properties);
	storage = StorageFactory.getStorageInstance(configuration);
	cleanTestStorageDirectory();
	startStore();
    }

    protected static void startStore() {
	assertNull("Engine was started already.", store);
	store = new LogStructuredStoreImpl(storage, new File("lss"), 1024 * 1024, 10 * 1024 * 1024, 8192, 3);
	store.open();
    }

    private static void cleanTestStorageDirectory() throws FileNotFoundException, IOException {
	for (File file : storage.list(new File("/"))) {
	    FileStatus fileStatus = storage.getFileStatus(file);
	    if (fileStatus.isDirectory()) {
		storage.removeDirectory(file, true);
	    } else {
		storage.delete(file);
	    }
	}
    }

    @AfterClass
    public static void cleanupStorageEngine() throws IOException {
	stopStore();
    }

    protected static void stopStore() throws IOException {
	assertNotNull("Engine was not started, yet.", store);
	store.close();
	store = null;
    }

    protected LogStructuredStore getStore() {
	return store;
    }

    protected Set<File> getCommitLogs(Storage storage, File directory) {
	Set<File> commitLogs = new HashSet<>();
	for (File commitLog : storage.list(directory, new CurrentCommitLogFilenameFilter(storage))) {
	    commitLogs.add(commitLog);
	}
	return commitLogs;
    }

}
