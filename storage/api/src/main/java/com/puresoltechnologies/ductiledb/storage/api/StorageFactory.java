package com.puresoltechnologies.ductiledb.storage.api;

import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

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

    private static StorageFactoryService storageFactoryService = null;

    public static Storage create(Map<String, String> configuration) throws StorageFactoryServiceException {
	if (storageFactoryService == null) {
	    loadStorageFactoryService();
	}
	return storageFactoryService.create(configuration);
    }

    private static synchronized void loadStorageFactoryService() throws StorageFactoryServiceException {
	if (storageFactoryService == null) {
	    ServiceLoader<StorageFactoryService> storageFactoryServiceLoader = ServiceLoader
		    .load(StorageFactoryService.class);
	    Iterator<StorageFactoryService> iterator = storageFactoryServiceLoader.iterator();
	    if (!iterator.hasNext()) {
		throw new StorageFactoryServiceException("No StorageFactoryService implementation was found.");
	    }
	    storageFactoryService = iterator.next();
	    if (iterator.hasNext()) {
		throw new StorageFactoryServiceException(
			"Multiple StorageFactoryService implementations were found. Only one implementation can be used and has to be provided.");
	    }
	}
    }

}
