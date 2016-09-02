package com.puresoltechnologies.ductiledb.storage.engine.cf.index.secondary;

import java.io.IOException;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.lss.LogStructuredStoreImpl;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class SecondaryIndexEngineImpl extends LogStructuredStoreImpl implements SecondaryIndexEngine {

    private final SecondaryIndexDescriptor indexDescription;

    public SecondaryIndexEngineImpl(Storage storage, SecondaryIndexDescriptor indexDescriptor, long maxCommitLogSize,
	    long maxDataFileSize, int bufferSize, int maxFileGenerations) {
	super(storage, indexDescriptor.getDirectory(), maxCommitLogSize, maxDataFileSize, bufferSize,
		maxFileGenerations);
	this.indexDescription = indexDescriptor;
    }

    @Override
    public final SecondaryIndexDescriptor getDescription() {
	return indexDescription;
    }

    public void drop() throws StorageException {
	try {
	    getStorage().removeDirectory(getDirectory(), true);
	} catch (IOException e) {
	    throw new StorageException("Could not drop secondary index.", e);
	}
    }

}
