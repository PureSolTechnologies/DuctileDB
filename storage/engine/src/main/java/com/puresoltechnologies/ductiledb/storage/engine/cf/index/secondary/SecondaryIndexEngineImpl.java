package com.puresoltechnologies.ductiledb.storage.engine.cf.index.secondary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnKeySet;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnMap;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
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

    public void drop() {
	try {
	    getStorage().removeDirectory(getDirectory(), true);
	} catch (IOException e) {
	    throw new StorageException("Could not drop secondary index.", e);
	}
    }

    public byte[] createRowKey(byte[] rowKey, ColumnMap columnMap) {
	ColumnKeySet columns = indexDescription.getColumns();
	List<byte[]> values = new ArrayList<>();
	int keySize = 0;
	for (byte[] column : columns) {
	    byte[] value = columnMap.get(column).getValue();
	    keySize += 4 + value.length;
	    values.add(value);
	}
	keySize += 4 + rowKey.length;
	byte[] indexKey = new byte[keySize];
	int pos = 0;
	for (byte[] value : values) {
	    System.arraycopy(Bytes.toBytes(value.length), 0, indexKey, pos, 4);
	    pos += 4;
	    System.arraycopy(value, 0, indexKey, pos, value.length);
	    pos += value.length;
	}
	System.arraycopy(Bytes.toBytes(rowKey.length), 0, indexKey, pos, 4);
	pos += 4;
	System.arraycopy(rowKey, 0, indexKey, pos, rowKey.length);
	return indexKey;
    }

}
