package com.puresoltechnologies.ductiledb.logstore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import com.puresoltechnologies.ductiledb.storage.spi.FileStatus;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;
import com.puresoltechnologies.ductiledb.storage.spi.StorageConfiguration;

/**
 * This class contains functionality and factories for
 * {@link LogStructuredStore} tests and tests which are based on it.
 * 
 * @author Rick-Rainer Ludwig
 */
public class LogStructuredStoreTestUtils {

    public static StorageConfiguration createStorageConfiguration() {
	StorageConfiguration configuration = new StorageConfiguration();
	configuration.setBlockSize(8192);
	Properties properties = new Properties();
	properties.put("storage.os.directory", "target/test");
	configuration.setProperties(properties);
	return configuration;
    }

    public static LogStoreConfiguration createConfiguration() {
	LogStoreConfiguration storeConfiguration = new LogStoreConfiguration();
	storeConfiguration.setMaxDataFileSize(10 * 1024 * 1024);
	storeConfiguration.setMaxCommitLogSize(1024 * 1024);
	storeConfiguration.setBufferSize(8192);
	storeConfiguration.setMaxFileGenerations(3);
	return storeConfiguration;
    }

    public static void cleanTestStorageDirectory(Storage storage) throws FileNotFoundException, IOException {
	for (File file : storage.list(new File("/"))) {
	    FileStatus fileStatus = storage.getFileStatus(file);
	    if (fileStatus.isDirectory()) {
		storage.removeDirectory(file, true);
	    } else {
		storage.delete(file);
	    }
	}
    }

    public static LogStructuredStore createStore(Storage storage, LogStoreConfiguration configuration)
	    throws IOException {
	return LogStructuredStore.create(storage, new File("lss"), configuration);
    }

}
