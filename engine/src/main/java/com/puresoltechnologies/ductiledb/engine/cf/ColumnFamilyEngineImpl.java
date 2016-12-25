package com.puresoltechnologies.ductiledb.engine.cf;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.engine.DatabaseEngineConfiguration;
import com.puresoltechnologies.ductiledb.engine.cf.index.secondary.IndexType;
import com.puresoltechnologies.ductiledb.engine.cf.index.secondary.IndexedColumnFamilyScannerImpl;
import com.puresoltechnologies.ductiledb.engine.cf.index.secondary.SecondaryIndexDescriptor;
import com.puresoltechnologies.ductiledb.engine.cf.index.secondary.SecondaryIndexEngine;
import com.puresoltechnologies.ductiledb.engine.cf.index.secondary.SecondaryIndexEngineImpl;
import com.puresoltechnologies.ductiledb.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.engine.schema.TableDescriptor;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.logstore.LogStructuredStoreImpl;
import com.puresoltechnologies.ductiledb.logstore.RowScanner;
import com.puresoltechnologies.ductiledb.logstore.utils.Bytes;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This class handles the storage of a single column family.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class ColumnFamilyEngineImpl implements ColumnFamilyEngine {

    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyEngineImpl.class);

    private final LogStructuredStoreImpl store;

    private final ColumnFamilyDescriptor columnFamilyDescriptor;
    private final File indexDirectory;
    private final Map<String, SecondaryIndexEngineImpl> indizes = new HashMap<>();
    private final Map<String, SecondaryIndexDescriptor> indexDescriptors = new HashMap<>();

    public ColumnFamilyEngineImpl(Storage storage, ColumnFamilyDescriptor columnFamilyDescriptor,
	    DatabaseEngineConfiguration configuration) {
	this.store = new LogStructuredStoreImpl(storage, //
		columnFamilyDescriptor.getDirectory(), //
		configuration.getMaxCommitLogSize(), //
		configuration.getMaxDataFileSize(), //
		configuration.getBufferSize(), //
		configuration.getMaxFileGenerations());
	this.columnFamilyDescriptor = columnFamilyDescriptor;
	this.indexDirectory = new File(store.getDirectory(), "indizes");
	open();
    }

    @Override
    public final Key getName() {
	return columnFamilyDescriptor.getName();
    }

    @Override
    public final ColumnFamilyDescriptor getDescriptor() {
	return columnFamilyDescriptor;
    }

    public final File getIndexDirectory() {
	return indexDirectory;
    }

    public void readIndizes() {
	Storage storage = store.getStorage();
	try {
	    storage.createDirectory(indexDirectory);
	} catch (IOException e) {
	    throw new StorageException("Could not create index directory '" + indexDirectory + "'.", e);
	}
	Iterable<File> list = storage.list(indexDirectory);
	for (File directory : list) {
	    if (storage.isDirectory(directory)) {
		readIndex(storage, directory);
	    }
	}
    }

    private void readIndex(Storage storage, File directory) {
	SecondaryIndexDescriptor secondaryIndexDescriptor = readSecondaryIndexDescriptor(directory);
	SecondaryIndexEngineImpl indexStore = new SecondaryIndexEngineImpl(storage, secondaryIndexDescriptor,
		store.getMaxCommitLogSize(), store.getMaxDataFileSize(), store.getBufferSize(),
		store.getMaxFileGenerations());
	indexStore.open();
	indizes.put(secondaryIndexDescriptor.getName(), indexStore);
	indexDescriptors.put(secondaryIndexDescriptor.getName(), secondaryIndexDescriptor);
    }

    private SecondaryIndexDescriptor readSecondaryIndexDescriptor(File directory) {
	Storage storage = store.getStorage();
	File metadataFile = new File(directory, "metadata.properties");
	try (BufferedInputStream metadata = storage.open(metadataFile)) {
	    Properties properties = new Properties();
	    properties.load(metadata);
	    ColumnKeySet columns = new ColumnKeySet();
	    IndexType indexType = IndexType.valueOf(properties.getProperty("index.type"));
	    int count = Integer.parseInt(properties.getProperty("index.columns.count"));
	    for (int id = 0; id < count; ++id) {
		String hexName = properties.getProperty("index.columns." + String.valueOf(id));
		columns.add(Key.fromHexString(hexName));
	    }
	    SecondaryIndexDescriptor secondaryIndexDescriptor = new SecondaryIndexDescriptor(directory.getName(),
		    columnFamilyDescriptor, columns, indexType);
	    return secondaryIndexDescriptor;
	} catch (IOException e) {
	    throw new StorageException("Could not read secondary index meta data.", e);
	}
    }

    @Override
    public void open() {
	store.open();
	readIndizes();
    }

    @Override
    public void close() throws Exception {
	store.close();
    }

    @Override
    public String toString() {
	TableDescriptor table = columnFamilyDescriptor.getTable();
	return "CFEngine:" + table.getNamespace().getName() + "." + table.getName() + "/"
		+ columnFamilyDescriptor.getName();
    }

    @Override
    public ColumnMap get(Key rowKey) {
	return store.get(rowKey);
    }

    @Override
    public RowScanner getScanner(Key startRowKey, Key endRowKey) {
	return store.getScanner(startRowKey, endRowKey);
    }

    @Override
    public void put(Key rowKey, ColumnMap columnMap) {
	store.put(rowKey, columnMap);
	if (hasIndizes()) {
	    for (Entry<String, SecondaryIndexDescriptor> indexDescriptorEntry : indexDescriptors.entrySet()) {
		if (indexDescriptorEntry.getValue().matchesColumns(columnMap.getColumnKeySet())) {
		    addToIndex(indexDescriptorEntry.getValue(), rowKey, columnMap);
		}
	    }
	}
    }

    private void addToIndex(SecondaryIndexDescriptor value, Key rowKey, ColumnMap columnMap) {
	SecondaryIndexEngineImpl indexEngine = indizes.get(value.getName());
	Key indexRowKey = indexEngine.createRowKey(rowKey, columnMap);
	if (value.getIndexType() == IndexType.HEAP) {
	    ColumnMap values = new ColumnMap();
	    byte[] keyBytes = rowKey.getBytes();
	    values.put(Key.of("key"), ColumnValue.of(keyBytes));
	    indexEngine.put(indexRowKey, values);
	} else {
	    indexEngine.put(indexRowKey, columnMap);
	}
    }

    @Override
    public void delete(Key rowKey) {
	store.delete(rowKey);
	if (hasIndizes()) {
	    ColumnMap columnMap = get(rowKey);
	    for (Entry<String, SecondaryIndexDescriptor> indexDescriptorEntry : indexDescriptors.entrySet()) {
		if (indexDescriptorEntry.getValue().matchesColumns(columnMap.getColumnKeySet())) {
		    removeFromIndex(indexDescriptorEntry.getValue(), rowKey, columnMap);
		}
	    }
	}
    }

    @Override
    public void delete(Key rowKey, Set<Key> columns) {
	store.delete(rowKey, columns);
	if (hasIndizes()) {
	    ColumnMap columnMap = get(rowKey);
	    for (Entry<String, SecondaryIndexDescriptor> indexDescriptorEntry : indexDescriptors.entrySet()) {
		if (indexDescriptorEntry.getValue().matchesColumns(new ColumnKeySet(columns))) {
		    removeFromIndex(indexDescriptorEntry.getValue(), rowKey, columnMap);
		}
	    }
	}
    }

    private void removeFromIndex(SecondaryIndexDescriptor value, Key rowKey, ColumnMap columnMap) {
	SecondaryIndexEngineImpl indexEngine = indizes.get(value.getName());
	Key indexRowKey = indexEngine.createRowKey(rowKey, columnMap);
	indexEngine.delete(indexRowKey);

    }

    private boolean hasIndizes() {
	return !indizes.isEmpty();
    }

    @Override
    public RowScanner find(Key columnKey, ColumnValue value) {
	SecondaryIndexEngine indexEngine = findIndexEngine(columnKey);
	if (indexEngine == null) {
	    return null;
	}
	ColumnValue fromValue = convertToFromValue(value);
	ColumnValue toValue = convertToToValue(value);
	return new IndexedColumnFamilyScannerImpl(this, indexEngine, fromValue, toValue);
    }

    private ColumnValue convertToFromValue(ColumnValue value) {
	byte[] bytes = value.getBytes();
	byte[] fromValue = new byte[bytes.length + 4];
	System.arraycopy(Bytes.toBytes(bytes.length), 0, fromValue, 0, 4);
	System.arraycopy(bytes, 0, fromValue, 4, bytes.length);
	return ColumnValue.of(fromValue);
    }

    private ColumnValue convertToToValue(ColumnValue value) {
	byte[] bytes = value.getBytes();
	byte[] toValue = new byte[bytes.length + 5];
	System.arraycopy(Bytes.toBytes(bytes.length), 0, toValue, 0, 4);
	System.arraycopy(bytes, 0, toValue, 4, bytes.length);
	toValue[4 + bytes.length] = (byte) 0xFF;
	return ColumnValue.of(toValue);
    }

    @Override
    public RowScanner find(Key columnKey, ColumnValue fromValue, ColumnValue toValue) {
	SecondaryIndexEngine indexEngine = findIndexEngine(columnKey);
	if (indexEngine == null) {
	    return null;
	}
	return new IndexedColumnFamilyScannerImpl(this, indexEngine, convertToFromValue(fromValue),
		convertToToValue(toValue));
    }

    public SecondaryIndexEngine findIndexEngine(Key columnKey) {
	if (indizes.isEmpty()) {
	    return null;
	}
	SecondaryIndexEngineImpl indexEngine = null;
	for (SecondaryIndexDescriptor indexEngineDescriptor : indexDescriptors.values()) {
	    if (indexEngineDescriptor.matchesColumns(columnKey)) {
		if (indexEngine != null) {
		    throw new IllegalStateException("Multiple indizes were found for this column combination.");
		}
		indexEngine = indizes.get(indexEngineDescriptor.getName());
	    }
	}
	return indexEngine;
    }

    @Override
    public long incrementColumnValue(Key rowKey, Key column, long incrementValue) {
	return incrementColumnValue(rowKey, column, 1l, incrementValue);
    }

    @Override
    public long incrementColumnValue(Key rowKey, Key column, long startValue, long incrementValue) {
	long result = startValue;
	store.getWriteLock().lock();
	try {
	    ColumnMap columnMap = get(rowKey);
	    if (columnMap != null) {
		ColumnValue oldValueBytes = columnMap.get(column);
		if (oldValueBytes != null) {
		    long oldValue = Bytes.toLong(oldValueBytes.getBytes());
		    result = oldValue + incrementValue;
		}
	    } else {
		columnMap = new ColumnMap();
	    }
	    columnMap.put(column, ColumnValue.of(Bytes.toBytes(result)));
	    store.writeCommitLog(rowKey, null, columnMap);
	} finally {
	    store.getWriteLock().unlock();
	}
	return result;
    }

    @Override
    public void createIndex(SecondaryIndexDescriptor indexDescriptor) {
	logger.info("Creating new index '" + indexDescriptor + "' for '" + toString() + "'...");
	Storage storage = store.getStorage();
	File indexDirectory = new File(getIndexDirectory(), indexDescriptor.getName());
	if (storage.exists(indexDirectory)) {
	    throw new StorageException("Index with name '" + indexDescriptor.getName() + "' exists already.");
	}
	try {
	    storage.createDirectory(indexDirectory);
	    File metadataFile = new File(indexDirectory, "metadata.properties");
	    try (BufferedOutputStream metadata = storage.create(metadataFile)) {
		Properties properties = new Properties();
		int id = 0;
		ColumnKeySet columns = indexDescriptor.getColumns();
		properties.put("index.type", indexDescriptor.getIndexType().name());
		properties.put("index.columns.count", String.valueOf(columns.size()));
		for (Key column : columns) {
		    properties.put("index.columns." + String.valueOf(id), column.toHexString());
		    id++;
		}
		properties.store(metadata, "Column configuration of index.");
	    }
	} catch (IOException e) {
	    throw new StorageException("Could not create index with name '" + indexDescriptor.getName() + "'.", e);
	}
	SecondaryIndexEngineImpl indexStore = new SecondaryIndexEngineImpl(storage, indexDescriptor,
		store.getMaxCommitLogSize(), store.getMaxDataFileSize(), store.getBufferSize(),
		store.getMaxFileGenerations());
	indexStore.open();
	indizes.put(indexDescriptor.getName(), indexStore);
	indexDescriptors.put(indexDescriptor.getName(), indexDescriptor);
	logger.info("Index '" + indexDescriptor + "' for '" + toString() + "' created.");
    }

    @Override
    public void dropIndex(String name) {
	SecondaryIndexEngineImpl indexStore = indizes.get(name);
	if (indexStore != null) {
	    indexStore.drop();
	    indizes.remove(name);
	    indexDescriptors.remove(name);
	}
    }

    @Override
    public SecondaryIndexDescriptor getIndex(String name) {
	return indexDescriptors.get(name);
    }

    @Override
    public Iterable<SecondaryIndexDescriptor> getIndizes() {
	return indexDescriptors.values();
    }

}
