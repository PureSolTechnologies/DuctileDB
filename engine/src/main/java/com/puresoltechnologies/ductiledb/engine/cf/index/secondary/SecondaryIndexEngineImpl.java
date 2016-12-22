package com.puresoltechnologies.ductiledb.engine.cf.index.secondary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.puresoltechnologies.ductiledb.engine.Key;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnKeySet;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnMap;
import com.puresoltechnologies.ductiledb.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.engine.lss.LogStructuredStoreImpl;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
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

    public void drop() {
	try {
	    getStorage().removeDirectory(getDirectory(), true);
	} catch (IOException e) {
	    throw new StorageException("Could not drop secondary index.", e);
	}
    }

    public Key createRowKey(Key rowKey, ColumnMap columnMap) {
	ColumnKeySet columns = indexDescription.getColumns();
	List<byte[]> values = new ArrayList<>();
	int keySize = 0;
	for (Key column : columns) {
	    byte[] value = columnMap.get(column).getBytes();
	    keySize += 4 + value.length;
	    values.add(value);
	}
	byte[] key = rowKey.getBytes();
	keySize += 4 + key.length;
	byte[] indexKey = new byte[keySize];
	int pos = 0;
	for (byte[] value : values) {
	    System.arraycopy(Bytes.toBytes(value.length), 0, indexKey, pos, 4);
	    pos += 4;
	    System.arraycopy(value, 0, indexKey, pos, value.length);
	    pos += value.length;
	}
	System.arraycopy(Bytes.toBytes(key.length), 0, indexKey, pos, 4);
	pos += 4;
	System.arraycopy(key, 0, indexKey, pos, key.length);
	return Key.of(indexKey);
    }

}
