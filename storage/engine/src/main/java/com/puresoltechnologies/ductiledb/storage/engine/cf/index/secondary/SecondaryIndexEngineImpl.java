package com.puresoltechnologies.ductiledb.storage.engine.cf.index.secondary;

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

}
