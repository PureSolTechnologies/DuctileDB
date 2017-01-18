package com.puresoltechnologies.ductiledb.logstore;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.puresoltechnologies.ductiledb.logstore.io.CurrentCommitLogFilenameFilter;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.api.StorageFactory;
import com.puresoltechnologies.ductiledb.storage.api.StorageFactoryServiceException;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;
import com.puresoltechnologies.ductiledb.storage.spi.StorageConfiguration;

public abstract class AbstractLogStructuredStoreTest {

    private static Storage storage = null;
    private static LogStructuredStore store = null;

    @BeforeClass
    public static void startStore() throws IOException, StorageFactoryServiceException, StorageException {
	StorageConfiguration configuration = LogStructuredStoreTestUtils.createStorageConfiguration();
	storage = StorageFactory.getStorageInstance(configuration);
	LogStructuredStoreTestUtils.cleanTestStorageDirectory(storage);

	assertNull("Engine was started already.", store);
	LogStoreConfiguration storeConfiguration = LogStructuredStoreTestUtils.createConfiguration();
	store = LogStructuredStoreTestUtils.createStore(storage, storeConfiguration);
	store.open();
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
