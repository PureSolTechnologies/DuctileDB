package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

import com.puresoltechnologies.ductiledb.storage.spi.Storage;
import com.puresoltechnologies.ductiledb.storage.spi.StorageFactory;

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
     * @throws IOException
     */
    public static StorageEngine create(Map<String, String> configuration, String storageName) throws IOException {
	ServiceLoader<StorageFactory> storages = ServiceLoader.load(StorageFactory.class);
	Iterator<StorageFactory> storagesIterator = storages.iterator();
	if (!storagesIterator.hasNext()) {
	    throw new IllegalStateException("Could not find any storage service.");
	}
	StorageFactory storageFactory = storagesIterator.next();
	if (storagesIterator.hasNext()) {
	    throw new IllegalStateException("Find multiple storage services.");
	}
	Storage storage = storageFactory.create(configuration);
	storage.initialize();
	return new StorageEngine(storage, storageName);
    }

}
