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

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + (int) (maxFileSize ^ (maxFileSize >>> 32));
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
	BlobStoreConfiguration other = (BlobStoreConfiguration) obj;
	if (maxFileSize != other.maxFileSize)
	    return false;
	return true;
    }

}
