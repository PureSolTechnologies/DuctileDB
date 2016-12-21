package com.puresoltechnologies.ductiledb.blobstore;

/**
 * This class keeps the configuration for the BLOB store.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class BlobStoreConfiguration {

    private long maxFileSize = 10485760; // 10MB
    private int chunkSize = 64 * 1024; // 64kB

    public long getMaxFileSize() {
	return maxFileSize;
    }

    public void setMaxFileSize(long maxFileSize) {
	this.maxFileSize = maxFileSize;
    }

    public int getChunkSize() {
	return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
	this.chunkSize = chunkSize;
    }

}
