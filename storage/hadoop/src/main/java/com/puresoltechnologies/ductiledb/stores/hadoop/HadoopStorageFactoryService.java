package com.puresoltechnologies.ductiledb.stores.hadoop;

import java.util.Map;

import com.puresoltechnologies.ductiledb.storage.spi.Storage;
import com.puresoltechnologies.ductiledb.storage.spi.StorageFactoryService;

public class HadoopStorageFactoryService implements StorageFactoryService {

    @Override
    public Storage create(Map<String, String> configuration) {
	return new HadoopStorage(configuration);
    }

}
