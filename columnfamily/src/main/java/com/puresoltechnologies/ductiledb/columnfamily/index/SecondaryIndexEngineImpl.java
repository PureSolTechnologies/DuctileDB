package com.puresoltechnologies.ductiledb.columnfamily.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.puresoltechnologies.ductiledb.columnfamily.ColumnKeySet;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnMap;
import com.puresoltechnologies.ductiledb.commons.Bytes;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.logstore.LogStoreConfiguration;
import com.puresoltechnologies.ductiledb.logstore.LogStructuredStore;
import com.puresoltechnologies.ductiledb.logstore.LogStructuredStoreImpl;
import com.puresoltechnologies.ductiledb.logstore.RowScanner;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class SecondaryIndexEngineImpl implements SecondaryIndexEngine {

    private final Storage storage;
    private final SecondaryIndexDescriptor indexDescription;
    private final LogStructuredStoreImpl store;

    public SecondaryIndexEngineImpl(Storage storage, SecondaryIndexDescriptor indexDescriptor,
	    LogStoreConfiguration configuration) throws IOException {
	this.storage = storage;
	this.store = (LogStructuredStoreImpl) LogStructuredStore.create(storage, indexDescriptor.getDirectory(),
		configuration);
	this.indexDescription = indexDescriptor;
    }

    @Override
    public final SecondaryIndexDescriptor getDescription() {
	return indexDescription;
    }

    public void drop() {
	try {
	    storage.removeDirectory(indexDescription.getDirectory(), true);
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
	    System.arraycopy(Bytes.fromInt(value.length), 0, indexKey, pos, 4);
	    pos += 4;
	    System.arraycopy(value, 0, indexKey, pos, value.length);
	    pos += value.length;
	}
	System.arraycopy(Bytes.fromInt(key.length), 0, indexKey, pos, 4);
	pos += 4;
	System.arraycopy(key, 0, indexKey, pos, key.length);
	return Key.of(indexKey);
    }

    @Override
    public void open() {
	store.open();
    }

    @Override
    public void close() {
	store.close();
    }

    @Override
    public void put(Key rowKey, byte[] columnValues) {
	store.put(rowKey, columnValues);
    }

    @Override
    public byte[] get(Key rowKey) {
	return store.get(rowKey);
    }

    @Override
    public RowScanner getScanner(Key startRowKey, Key endRowKey) {
	return store.getScanner(startRowKey, endRowKey);
    }

    @Override
    public void delete(Key rowKey) {
	store.delete(rowKey);
    }

}
