package com.puresoltechnologies.ductiledb.stores.hadoop;

import java.util.Map;

import com.puresoltechnologies.ductiledb.storage.spi.Storage;
import com.puresoltechnologies.ductiledb.storage.spi.StorageFactory;

public class HadoopStorageFactory implements StorageFactory {

    @Override
    public Storage create(Map<String, String> configuration) {
	return new HadoopStorage(configuration);
    }

}
