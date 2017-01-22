package com.puresoltechnologies.ductiledb.storage.spi;

import java.util.Properties;

/**
 * This class keeps the generic storage configuration
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class StorageConfiguration {

    public static final int DEFAULT_BLOCKSIZE = 8192;

    private int blockSize = DEFAULT_BLOCKSIZE;
    private Properties properties = new Properties();

    public int getBlockSize() {
	return blockSize;
    }

    public void setBlockSize(int blockSize) {
	this.blockSize = blockSize;
    }

    public Properties getProperties() {
	return properties;
    }

    public void setProperties(Properties properties) {
	this.properties = properties;
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + blockSize;
	result = prime * result + ((properties == null) ? 0 : properties.hashCode());
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
	StorageConfiguration other = (StorageConfiguration) obj;
	if (blockSize != other.blockSize)
	    return false;
	if (properties == null) {
	    if (other.properties != null)
		return false;
	} else if (!properties.equals(other.properties))
	    return false;
	return true;
    }

    @Override
    public String toString() {
	return "blocksize: " + blockSize + " bytes; properties=" + properties;
    }
}
