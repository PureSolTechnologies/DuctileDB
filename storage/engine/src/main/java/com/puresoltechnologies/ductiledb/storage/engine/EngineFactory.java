package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;
import com.puresoltechnologies.ductiledb.storage.spi.StorageFactoryService;

/**
 * This is the central factory to create a storage factory.
 * 
 * @author Rick-Rainer Ludwig
 */
public class EngineFactory {

    /**
     * This method creates a new storage engine.
     * 
     * @param string
     * @param string2
     * @return
     * @throws StorageException
     * @throws IOException
     */
    public static StorageEngine create(Map<String, String> configuration, String storageName) throws StorageException {
	ServiceLoader<StorageFactoryService> storages = ServiceLoader.load(StorageFactoryService.class);
	Iterator<StorageFactoryService> storagesIterator = storages.iterator();
	if (!storagesIterator.hasNext()) {
	    throw new IllegalStateException("Could not find any storage service.");
	}
	StorageFactoryService storageFactory = storagesIterator.next();
	if (storagesIterator.hasNext()) {
	    throw new IllegalStateException("Find multiple storage services.");
	}
	Storage storage = storageFactory.create(configuration);
	try {
	    storage.initialize();
	} catch (IOException e) {
	    throw new StorageException("Could not initialize storage.", e);
	}
	return new StorageEngine(storage, storageName);
    }

}
