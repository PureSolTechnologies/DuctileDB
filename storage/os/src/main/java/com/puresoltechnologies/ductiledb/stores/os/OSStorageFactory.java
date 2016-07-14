package com.puresoltechnologies.ductiledb.stores.os;

import java.util.Map;

import com.puresoltechnologies.ductiledb.storage.spi.Storage;
import com.puresoltechnologies.ductiledb.storage.spi.StorageFactory;

/**
 * This method is used to initialize the storage.
 * 
 * @author Rick-Rainer Ludwig
 */
public class OSStorageFactory implements StorageFactory {

    @Override
    public Storage create(Map<String, String> configuration) {
	return new OSStorage(configuration);
    }

}
