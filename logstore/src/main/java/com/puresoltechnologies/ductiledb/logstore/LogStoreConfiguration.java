package com.puresoltechnologies.ductiledb.logstore;

import com.puresoltechnologies.ductiledb.storage.spi.StorageConfiguration;

/**
 * This class contains the settings for the log structured store.
 * 
 * @author Rick-Rainer Ludwig
 */
public class LogStoreConfiguration {

    private static final long ONE_MEGABYTE = 1024 * 1024;
    private static final long ONE_GIGABYTE = 1024 * ONE_MEGABYTE;

    private long maxCommitLogSize = ONE_MEGABYTE;
    private long maxDataFileSize = ONE_GIGABYTE;
    private int bufferSize = -1;
    private int maxFileGenerations = 3;

    public long getMaxCommitLogSize() {
	return maxCommitLogSize;
    }

    public void setMaxCommitLogSize(long maxCommitLogSize) {
	this.maxCommitLogSize = maxCommitLogSize;
    }

    public long getMaxDataFileSize() {
	return maxDataFileSize;
    }

    public void setMaxDataFileSize(long maxDataFileSize) {
	this.maxDataFileSize = maxDataFileSize;
    }

    public void setBufferSize(int bufferSize) {
	this.bufferSize = bufferSize;
    }

    public int getBufferSize() {
	if (bufferSize <= 0) {
	    int blockSize = StorageConfiguration.DEFAULT_BLOCKSIZE;
	    return ((int) (getMaxCommitLogSize() / 10) / blockSize) * blockSize;
	} else {
	    return bufferSize;
	}
    }

    public int getMaxFileGenerations() {
	return maxFileGenerations;
    }

    public void setMaxFileGenerations(int maxFileGenerations) {
	this.maxFileGenerations = maxFileGenerations;
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + bufferSize;
	result = prime * result + (int) (maxCommitLogSize ^ (maxCommitLogSize >>> 32));
	result = prime * result + (int) (maxDataFileSize ^ (maxDataFileSize >>> 32));
	result = prime * result + maxFileGenerations;
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
	LogStoreConfiguration other = (LogStoreConfiguration) obj;
	if (bufferSize != other.bufferSize)
	    return false;
	if (maxCommitLogSize != other.maxCommitLogSize)
	    return false;
	if (maxDataFileSize != other.maxDataFileSize)
	    return false;
	if (maxFileGenerations != other.maxFileGenerations)
	    return false;
	return true;
    }

}
