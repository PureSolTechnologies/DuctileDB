package com.puresoltechnologies.ductiledb.stores.hadoop;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.storage.spi.Storage;
import com.puresoltechnologies.ductiledb.storage.spi.StorageFactoryService;

public class HadoopStorageFactoryService implements StorageFactoryService {

    private static final Logger logger = LoggerFactory.getLogger(HadoopStorageFactoryService.class);

    @Override
    public Storage create(Map<String, String> configuration) {
	logger.info("Creating HadoopStorage for configuration '" + configuration + "'...");
	return new HadoopStorage(configuration);
    }

}
