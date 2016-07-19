package com.puresoltechnologies.ductiledb.storage.spi;

import java.util.Properties;

/**
 * This class keeps the generic storage configuration
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class StorageConfiguration {

    private int blockSize = 4096;
    private Properties properties;

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

}
