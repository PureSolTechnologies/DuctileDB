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

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((logStoreConfiguration == null) ? 0 : logStoreConfiguration.hashCode());
	result = prime * result + ((storage == null) ? 0 : storage.hashCode());
	return result;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (getClass() != obj.getClass())
	    return false;
	BigTableConfiguration other = (BigTableConfiguration) obj;
	if (logStoreConfiguration == null) {
	    if (other.logStoreConfiguration != null)
		return false;
	} else if (!logStoreConfiguration.equals(other.logStoreConfiguration))
	    return false;
	if (storage == null) {
	    if (other.storage != null)
		return false;
	} else if (!storage.equals(other.storage))
	    return false;
	return true;
    }

}
