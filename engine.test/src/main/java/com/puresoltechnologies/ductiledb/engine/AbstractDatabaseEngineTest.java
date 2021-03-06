package com.puresoltechnologies.ductiledb.engine;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.puresoltechnologies.ductiledb.bigtable.BigTableConfiguration;
import com.puresoltechnologies.ductiledb.logstore.io.filter.CurrentCommitLogFilenameFilter;
import com.puresoltechnologies.ductiledb.logstore.utils.DefaultObjectMapper;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.api.StorageFactory;
import com.puresoltechnologies.ductiledb.storage.api.StorageFactoryServiceException;
import com.puresoltechnologies.ductiledb.storage.spi.FileStatus;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public abstract class AbstractDatabaseEngineTest {

    private static final String DATABASE_ENGINE_NAME = "DatabaseEngineTest";

    private static BigTableConfiguration configuration;

    private static Storage storage = null;
    private static DatabaseEngineImpl storageEngine = null;

    @BeforeClass
    public static void readConfiguration() throws IOException, StorageFactoryServiceException, StorageException {
	ObjectMapper objectMapper = DefaultObjectMapper.getInstance();
	try (InputStream inputStream = AbstractDatabaseEngineTest.class.getResourceAsStream("/database-engine.json")) {
	    configuration = objectMapper.readValue(inputStream, BigTableConfiguration.class);
	    assertNotNull(configuration);
	}
	storage = StorageFactory.getStorageInstance(configuration.getStorage());
	cleanTestStorageDirectory();
	startEngine();
    }

    protected static void startEngine() throws IOException {
	assertNull("Engine was started already.", storageEngine);
	storageEngine = new DatabaseEngineImpl(storage, new File("/AbstractDatabaseEngineTest"), DATABASE_ENGINE_NAME,
		configuration);
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

    @Before
    public void cleanup() throws IOException {
	cleanTestStorageDirectory();
    }

    @AfterClass
    public static void cleanupStorageEngine() throws IOException {
	stopEngine();
    }

    protected static void stopEngine() throws IOException {
	assertNotNull("Engine was not started, yet.", storageEngine);
	storageEngine.close();
	storageEngine = null;
    }

    public static BigTableConfiguration getConfiguration() {
	return configuration;
    }

    public static void setConfiguration(BigTableConfiguration configuration) {
	AbstractDatabaseEngineTest.configuration = configuration;
    }

    protected DatabaseEngineImpl getEngine() {
	return storageEngine;
    }

    protected Set<File> getCommitLogs(Storage storage, File directory) {
	Set<File> commitLogs = new HashSet<>();
	for (File commitLog : storage.list(directory, new CurrentCommitLogFilenameFilter(storage))) {
	    commitLogs.add(commitLog);
	}
	return commitLogs;
    }

}
