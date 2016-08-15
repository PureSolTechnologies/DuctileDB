package com.puresoltechnologies.ductiledb.storage.engine;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.yaml.snakeyaml.Yaml;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.api.StorageFactory;
import com.puresoltechnologies.ductiledb.storage.api.StorageFactoryServiceException;
import com.puresoltechnologies.ductiledb.storage.engine.io.CommitLogFilenameFilter;
import com.puresoltechnologies.ductiledb.storage.spi.FileStatus;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public abstract class AbstractDatabaseEngineTest {

    private static final String DATABASE_ENGINE_NAME = "DatabaseEngineTest";

    private static DatabaseEngineConfiguration configuration;

    private static Storage storage = null;
    private static DatabaseEngineImpl storageEngine = null;

    @BeforeClass
    public static void readConfiguration() throws IOException, StorageFactoryServiceException, StorageException {
	Yaml yaml = new Yaml();
	try (InputStream inputStream = AbstractDatabaseEngineTest.class.getResourceAsStream("/database-engine.yml")) {
	    configuration = yaml.loadAs(inputStream, DatabaseEngineConfiguration.class);
	    assertNotNull(configuration);
	}
	storage = StorageFactory.getStorageInstance(configuration.getStorage());
	cleanTestStorageDirectory();
	storageEngine = new DatabaseEngineImpl(storage, DATABASE_ENGINE_NAME, configuration);
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
	if (storageEngine != null) {
	    storageEngine.close();
	}
    }

    public static DatabaseEngineConfiguration getConfiguration() {
	return configuration;
    }

    public static void setConfiguration(DatabaseEngineConfiguration configuration) {
	AbstractDatabaseEngineTest.configuration = configuration;
    }

    protected DatabaseEngineImpl getEngine() {
	return storageEngine;
    }

    protected Set<File> getCommitLogs(Storage storage, File directory) {
	Set<File> commitLogs = new HashSet<>();
	for (File commitLog : storage.list(directory, new CommitLogFilenameFilter(storage))) {
	    commitLogs.add(commitLog);
	}
	return commitLogs;
    }

}
