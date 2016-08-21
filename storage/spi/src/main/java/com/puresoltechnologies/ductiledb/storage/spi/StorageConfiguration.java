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

}
