package com.puresoltechnologies.ductiledb.storage.spi;

import java.util.Map;

/**
 * This interface is used to implement the storage factories.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface StorageFactory {

    /**
     * Creates the actual Storage object base on the configuration provided.
     * 
     * @param configuration
     *            is a {@link Map} containing the settings.
     * @return A {@link Storage} is returned.
     */
    public Storage create(Map<String, String> configuration);
}
