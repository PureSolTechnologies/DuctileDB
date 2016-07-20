package com.puresoltechnologies.ductiledb.storage.engine;

import com.puresoltechnologies.ductiledb.storage.spi.StorageConfiguration;

/**
 * This class contains the settings for the storage engine.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DatabaseEngineConfiguration {

    private static final long ONE_MEGABYTE = 1024 * 1024;

    private int bufferSize = -1;
    private long maxCommitLogSize = ONE_MEGABYTE;
    private StorageConfiguration storage = new StorageConfiguration();

    public StorageConfiguration getStorage() {
	return storage;
    }

    public void setStorage(StorageConfiguration storage) {
	this.storage = storage;
    }

    public long getMaxCommitLogSize() {
	return maxCommitLogSize;
    }

    public void setMaxCommitLogSize(long maxCommitLogSize) {
	this.maxCommitLogSize = maxCommitLogSize;
    }

    public int getBufferSize() {
	if (bufferSize <= 0) {
	    int blockSize;
	    if (storage != null) {
		blockSize = storage.getBlockSize();
	    } else {
		blockSize = StorageConfiguration.DEFAULT_BLOCKSIZE;
	    }
	    return ((int) (getMaxCommitLogSize() / 10) / blockSize) * blockSize;
	} else {
	    return bufferSize;
	}
    }
}
