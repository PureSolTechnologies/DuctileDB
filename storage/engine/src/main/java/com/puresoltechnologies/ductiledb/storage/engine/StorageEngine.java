package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.IOException;

import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class StorageEngine {

    private final Storage storage;
    private final String storageName;

    public StorageEngine(Storage storage, String storageName) throws IOException {
	this.storage = storage;
	this.storageName = storageName;
	storage.createDirectory(storageName);
    }

    Storage getStorage() {
	return storage;
    }

}
