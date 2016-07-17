package com.puresoltechnologies.ductiledb.storage.api;

import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.storage.spi.Storage;
import com.puresoltechnologies.ductiledb.storage.spi.StorageFactoryService;

/**
 * This is the central class to access the configured storage implementation.
 * For the factory to work, there are two pre-requisites:
 * <ol>
 * <li>Exactly one implementation of {@link StorageFactoryService} needs to be
 * provided as SPI service.</li>
 * <li>In the DuctileDB configuration the needed settings for the provided
 * storage need to be present, otherwise the service will fail to load or the
 * storage will fail.
 * </ol>
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class StorageFactory {

    private static final Logger logger = LoggerFactory.getLogger(StorageFactory.class);

    private static Storage storage = null;

    /**
     * This method returns a storage instance which is a singleton.
     * 
     * @param configuration
     *            is the configuration to create the first instance of the
     *            storage.
     * @return A {@link Storage} is returned.
     * @throws StorageFactoryServiceException
     */
    public static Storage getStorageInstance(Map<String, String> configuration) throws StorageFactoryServiceException {
	if (storage == null) {
	    loadStorageFactoryService(configuration);
	}
	return storage;
    }

    private static synchronized void loadStorageFactoryService(Map<String, String> configuration)
	    throws StorageFactoryServiceException {
	if (storage == null) {
	    logger.info("Loading StorageFactoryService via SPI...");
	    ServiceLoader<StorageFactoryService> storageFactoryServiceLoader = ServiceLoader
		    .load(StorageFactoryService.class);
	    Iterator<StorageFactoryService> iterator = storageFactoryServiceLoader.iterator();
	    if (!iterator.hasNext()) {
		throw new StorageFactoryServiceException("No StorageFactoryService implementation was found.");
	    }
	    StorageFactoryService storageFactoryService = iterator.next();
	    if (iterator.hasNext()) {
		throw new StorageFactoryServiceException(
			"Multiple StorageFactoryService implementations were found. Only one implementation can be used and has to be provided.");
	    }
	    storage = storageFactoryService.create(configuration);
	    logger.info("Storage '" + storage.getClass().getName() + "' found.");
	}
    }

}
