package com.puresoltechnologies.ductiledb.bigtable;

import com.puresoltechnologies.ductiledb.logstore.LogStoreConfiguration;
import com.puresoltechnologies.ductiledb.storage.spi.StorageConfiguration;

/**
 * This class contains the settings for the storage engine.
 * 
 * @author Rick-Rainer Ludwig
 */
public class BigTableConfiguration {

    private LogStoreConfiguration logStoreConfiguration = new LogStoreConfiguration();
    private StorageConfiguration storage = new StorageConfiguration();

    public LogStoreConfiguration getLogStoreConfiguration() {
	return logStoreConfiguration;
    }

    public void setLogStoreConfiguration(LogStoreConfiguration logStoreConfiguration) {
	this.logStoreConfiguration = logStoreConfiguration;
    }

    public StorageConfiguration getStorage() {
	return storage;
    }

    public void setStorage(StorageConfiguration storage) {
	this.storage = storage;
    }
}
