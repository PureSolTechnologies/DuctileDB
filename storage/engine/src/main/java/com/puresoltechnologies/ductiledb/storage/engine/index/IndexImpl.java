package com.puresoltechnologies.ductiledb.storage.engine.index;

import java.io.File;
import java.io.IOException;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.ColumnFamilyEngineUtils;
import com.puresoltechnologies.ductiledb.storage.engine.io.DbFilenameFilter;
import com.puresoltechnologies.ductiledb.storage.engine.io.MetadataFilenameFilter;
import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class IndexImpl implements Index {

    private final Storage storage;
    private final ColumnFamilyDescriptor columnFamilyDescriptor;

    public IndexImpl(Storage storage, ColumnFamilyDescriptor columnFamilyDescriptor) {
	super();
	this.storage = storage;
	this.columnFamilyDescriptor = columnFamilyDescriptor;
    }

    @Override
    public void update() throws StorageException {
	try {
	    Iterable<File> listMetadata = storage.list(columnFamilyDescriptor.getDirectory(),
		    new MetadataFilenameFilter());
	    File latestMetadata = null;
	    for (File metadata : listMetadata) {
		if (latestMetadata.compareTo(metadata) < 0) {
		    latestMetadata = metadata;
		}
	    }
	    String timestamp = ColumnFamilyEngineUtils.extractTimestampForMetadataFile(latestMetadata.getName());
	    storage.list(columnFamilyDescriptor.getDirectory(), new DbFilenameFilter(timestamp));
	} catch (IOException e) {
	    throw new StorageException("Could not determine latest timestamp.");
	}
    }

    @Override
    public IndexEntry find(byte[] rowKey) {
	// TODO Auto-generated method stub
	return null;
    }

}
