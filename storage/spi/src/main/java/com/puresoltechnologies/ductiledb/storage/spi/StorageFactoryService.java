package com.puresoltechnologies.ductiledb.storage.spi;

/**
 * This interface is used to implement the storage factories.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface StorageFactoryService {

    /**
     * Creates the actual Storage object base on the configuration provided.
     * 
     * @param configuration
     *            is a {@link StorageConfiguration} containing the settings.
     * @return A {@link Storage} is returned.
     */
    public Storage create(StorageConfiguration configuration);
}
