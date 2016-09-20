package com.puresoltechnologies.ductiledb.storage.engine.cf;

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

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineConfiguration;
import com.puresoltechnologies.ductiledb.storage.engine.Key;
import com.puresoltechnologies.ductiledb.storage.engine.cf.index.secondary.IndexedColumnFamilyScannerImpl;
import com.puresoltechnologies.ductiledb.storage.engine.cf.index.secondary.SecondaryIndexDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.cf.index.secondary.SecondaryIndexEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.lss.LogStructuredStoreImpl;
import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.TableDescriptor;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This class handles the storage of a single column family.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class ColumnFamilyEngineImpl extends LogStructuredStoreImpl implements ColumnFamilyEngine {

    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyEngineImpl.class);

    private final ColumnFamilyDescriptor columnFamilyDescriptor;
    private final File indexDirectory;
    private final Map<String, SecondaryIndexEngineImpl> indizes = new HashMap<>();
    private final Map<String, SecondaryIndexDescriptor> indexDescriptors = new HashMap<>();

    public ColumnFamilyEngineImpl(Storage storage, ColumnFamilyDescriptor columnFamilyDescriptor,
	    DatabaseEngineConfiguration configuration) {
	super(storage, //
		columnFamilyDescriptor.getDirectory(), //
		configuration.getMaxCommitLogSize(), //
		configuration.getMaxDataFileSize(), //
		configuration.getBufferSize(), //
		configuration.getMaxFileGenerations());
	this.columnFamilyDescriptor = columnFamilyDescriptor;
	this.indexDirectory = new File(getDirectory(), "indizes");
	open();
    }

    @Override
    public final byte[] getName() {
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
	Storage storage = getStorage();
	try {
	    storage.createDirectory(indexDirectory);
	} catch (IOException e) {
	    throw new StorageException("Could not create index directory '" + indexDirectory + "'.", e);
	}
	Iterable<File> list = storage.list(indexDirectory);
	for (File directory : list) {
	    if (storage.isDirectory(directory)) {
		SecondaryIndexDescriptor secondaryIndexDescriptor = readSecondaryIndexDescriptor(directory);
	    }
	}
    }

    private SecondaryIndexDescriptor readSecondaryIndexDescriptor(File directory) {
	Storage storage = getStorage();
	File metadataFile = new File(directory, "metadata.properties");
	try (BufferedInputStream metadata = storage.open(metadataFile)) {
	    Properties properties = new Properties();
	    properties.load(metadata);
	    ColumnKeySet columns = new ColumnKeySet();
	    int count = Integer.parseInt(properties.getProperty("index.columns.count"));
	    for (int id = 0; id < count; ++id) {
		String hexName = properties.getProperty("index.columns." + String.valueOf(id));
		byte[] column = Bytes.fromHexString(hexName);
		columns.add(column);
	    }
	    SecondaryIndexDescriptor secondaryIndexDescriptor = new SecondaryIndexDescriptor(directory.getName(),
		    columnFamilyDescriptor, columns);
	    return secondaryIndexDescriptor;
	} catch (IOException e) {
	    throw new StorageException("Could not read secondary index meta data.", e);
	}
    }

    @Override
    public void open() {
	super.open();
	readIndizes();
    }

    @Override
    public String toString() {
	TableDescriptor table = columnFamilyDescriptor.getTable();
	return "CFEngine:" + table.getNamespace().getName() + "." + table.getName() + "/"
		+ Bytes.toHumanReadableString(columnFamilyDescriptor.getName());
    }

    @Override
    public void put(byte[] rowKey, ColumnMap columnMap) {
	super.put(rowKey, columnMap);
	if (hasIndizes()) {
	    for (Entry<String, SecondaryIndexDescriptor> indexDescriptorEntry : indexDescriptors.entrySet()) {
		if (indexDescriptorEntry.getValue().matchesColumns(columnMap.keySet())) {
		    addToIndex(indexDescriptorEntry.getValue(), rowKey, columnMap);
		}
	    }
	}
    }

    private void addToIndex(SecondaryIndexDescriptor value, byte[] rowKey, ColumnMap columnMap) {
	SecondaryIndexEngineImpl indexEngine = indizes.get(value.getName());
	byte[] indexRowKey = indexEngine.createRowKey(rowKey, columnMap);
	indexEngine.put(indexRowKey, new ColumnMap());
    }

    @Override
    public void delete(byte[] rowKey) {
	super.delete(rowKey);
	if (hasIndizes()) {
	    ColumnMap columnMap = get(rowKey);
	    for (Entry<String, SecondaryIndexDescriptor> indexDescriptorEntry : indexDescriptors.entrySet()) {
		if (indexDescriptorEntry.getValue().matchesColumns(columnMap.keySet())) {
		    removeFromIndex(indexDescriptorEntry.getValue(), rowKey, columnMap);
		}
	    }
	}
    }

    @Override
    public void delete(byte[] rowKey, Set<byte[]> columns) {
	super.delete(rowKey, columns);
	if (hasIndizes()) {
	    ColumnMap columnMap = get(rowKey);
	    for (Entry<String, SecondaryIndexDescriptor> indexDescriptorEntry : indexDescriptors.entrySet()) {
		if (indexDescriptorEntry.getValue().matchesColumns(columns)) {
		    removeFromIndex(indexDescriptorEntry.getValue(), rowKey, columnMap);
		}
	    }
	}
    }

    private void removeFromIndex(SecondaryIndexDescriptor value, byte[] rowKey, ColumnMap columnMap) {
	SecondaryIndexEngineImpl indexEngine = indizes.get(value.getName());
	byte[] indexRowKey = indexEngine.createRowKey(rowKey, columnMap);
	indexEngine.delete(indexRowKey);

    }

    private boolean hasIndizes() {
	return !indizes.isEmpty();
    }

    @Override
    public ColumnFamilyScanner find(byte[] columnKey, byte[] value) {
	SecondaryIndexEngineImpl indexEngine = indizes.get(columnKey);
	if (indizes == null) {
	    return null;
	}
	byte[] fromValue = value;
	byte[] toValue = new byte[value.length + 1];
	for (int i = 0; i < value.length; ++i) {
	    toValue[i] = value[i];
	}
	toValue[value.length] = (byte) 0xFF;
	return new IndexedColumnFamilyScannerImpl(this, indexEngine, fromValue, toValue);
    }

    @Override
    public ColumnFamilyScanner find(byte[] columnKey, byte[] fromValue, byte[] toValue) {
	SecondaryIndexEngineImpl indexEngine = indizes.get(columnKey);
	if (indizes == null) {
	    return null;
	}
	byte[] toValue2 = new byte[toValue.length + 1];
	for (int i = 0; i < toValue.length; ++i) {
	    toValue2[i] = toValue[i];
	}
	toValue2[toValue.length] = (byte) 0xFF;
	return new IndexedColumnFamilyScannerImpl(this, indexEngine, fromValue, toValue2);
    }

    @Override
    public long incrementColumnValue(byte[] rowKey, byte[] column, long incrementValue) {
	return incrementColumnValue(rowKey, column, 1l, incrementValue);
    }

    @Override
    public long incrementColumnValue(byte[] rowKey, byte[] column, long startValue, long incrementValue) {
	long result = startValue;
	getWriteLock().lock();
	try {
	    ColumnMap columnMap = get(rowKey);
	    if (columnMap != null) {
		ColumnValue oldValueBytes = columnMap.get(column);
		if (oldValueBytes != null) {
		    long oldValue = Bytes.toLong(oldValueBytes.getValue());
		    result = oldValue + incrementValue;
		}
	    } else {
		columnMap = new ColumnMap();
	    }
	    columnMap.put(column, new ColumnValue(Bytes.toBytes(result), null));
	    writeCommitLog(new Key(rowKey), null, columnMap);
	} finally {
	    getWriteLock().unlock();
	}
	return result;
    }

    @Override
    public void createIndex(SecondaryIndexDescriptor indexDescriptor) {
	logger.info("Creating new index '" + indexDescriptor + "' for '" + toString() + "'...");
	Storage storage = getStorage();
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
		properties.put("index.columns.count", String.valueOf(columns.size()));
		for (byte[] column : columns) {
		    properties.put("index.columns." + String.valueOf(id), Bytes.toHexString(column));
		    id++;
		}
		properties.store(metadata, "Column configuration of index.");
	    }
	} catch (IOException e) {
	    throw new StorageException("Could not create index with name '" + indexDescriptor.getName() + "'.", e);
	}
	SecondaryIndexEngineImpl indexStore = new SecondaryIndexEngineImpl(storage, indexDescriptor,
		getMaxCommitLogSize(), getMaxDataFileSize(), getBufferSize(), getMaxFileGenerations());
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
