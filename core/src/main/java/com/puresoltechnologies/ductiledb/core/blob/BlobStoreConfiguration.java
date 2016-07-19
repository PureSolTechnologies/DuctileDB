package com.puresoltechnologies.ductiledb.core.blob;

/**
 * This class keeps the configuration for the BLOB store.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class BlobStoreConfiguration {

    private long maxFileSize = 10485760; // 10MB

    public long getMaxFileSize() {
	return maxFileSize;
    }

    public void setMaxFileSize(long maxFileSize) {
	this.maxFileSize = maxFileSize;
    }

}
