package com.puresoltechnologies.ductiledb.bigtable;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.puresoltechnologies.commons.misc.StopWatch;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamily;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnFamilyImpl;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnMap;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnValue;
import com.puresoltechnologies.ductiledb.commons.Bytes;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.logstore.utils.DefaultObjectMapper;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This class is the central engine class for table storage. It is using the
 * {@link ColumnFamily} to store the separated column families.
 * 
 * @author Rick-Rainer Ludwig
 */
public class BigTableImpl implements BigTable {

    private static final Logger logger = LoggerFactory.getLogger(BigTable.class);

    private final Storage storage;
    private final TableDescriptor tableDescriptor;
    private final BigTableConfiguration configuration;
    private final TreeMap<Key, ColumnFamilyImpl> columnFamilyEngines = new TreeMap<>();

    BigTableImpl(Storage storage, TableDescriptor tableDescriptor, BigTableConfiguration configuration)
	    throws IOException {
	super();
	this.storage = storage;
	this.tableDescriptor = tableDescriptor;
	this.configuration = configuration;
	logger.info("Starting table engine '" + tableDescriptor.getName() + "'...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	storage.createDirectory(tableDescriptor.getDirectory());
	ObjectMapper objectMapper = DefaultObjectMapper.getInstance();
	try (BufferedOutputStream parameterFile = storage
		.create(new File(tableDescriptor.getDirectory(), "configuration.json"))) {
	    objectMapper.writeValue(parameterFile, configuration);
	}
	try (BufferedOutputStream parameterFile = storage
		.create(new File(tableDescriptor.getDirectory(), "descriptor.json"))) {
	    objectMapper.writeValue(parameterFile, tableDescriptor);
	}
	stopWatch.stop();
	logger.info("Table engine '" + tableDescriptor.getName() + "' started in " + stopWatch.getMillis() + "ms.");
    }

    BigTableImpl(Storage storage, File directory) throws IOException {
	super();
	this.storage = storage;
	ObjectMapper objectMapper = DefaultObjectMapper.getInstance();
	try (BufferedInputStream parameterFile = storage.open(new File(directory, "descriptor.json"))) {
	    this.tableDescriptor = objectMapper.readValue(parameterFile, TableDescriptor.class);
	}
	logger.info("Starting table engine '" + tableDescriptor.getName() + "'...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	try (BufferedInputStream parameterFile = storage.open(new File(directory, "configuration.json"))) {
	    this.configuration = objectMapper.readValue(parameterFile, BigTableConfiguration.class);
	}
	openColumnFamilies();
	stopWatch.stop();
	logger.info("Table engine '" + tableDescriptor.getName() + "' started in " + stopWatch.getMillis() + "ms.");
    }

    private void openColumnFamilies() throws IOException {
	Iterable<File> directories = storage.list(tableDescriptor.getDirectory());
	for (File directory : directories) {
	    if (storage.isDirectory(directory)) {
		ColumnFamilyImpl engine = (ColumnFamilyImpl) ColumnFamily.reopen(storage, directory);
		columnFamilyEngines.put(engine.getName(), engine);
	    }
	}
    }

    @Override
    public String getName() {
	return tableDescriptor.getName();
    }

    @Override
    public ColumnFamily addColumnFamily(Key name) throws IOException {
	ColumnFamilyImpl engine = (ColumnFamilyImpl) ColumnFamily.create(storage,
		new ColumnFamilyDescriptor(name,
			new File(tableDescriptor.getDirectory(), Bytes.toHexString(name.getBytes()))),
		configuration.getLogStoreConfiguration());
	columnFamilyEngines.put(name, engine);
	return engine;
    }

    @Override
    public void dropColumnFamily(Key name) {
	ColumnFamilyImpl columnFamilyEngine = columnFamilyEngines.get(name);
	if (columnFamilyEngine != null) {
	    columnFamilyEngines.remove(name);
	    columnFamilyEngine.close();
	}
    }

    @Override
    public ColumnFamily getColumnFamily(Key name) {
	return columnFamilyEngines.get(name);
    }

    @Override
    public boolean hasColumnFamily(Key name) {
	return columnFamilyEngines.containsKey(name);
    }

    public void setRunCompactions(boolean runCompaction) {
	columnFamilyEngines.values().forEach(engine -> engine.setRunCompactions(runCompaction));
    }

    public void runCompaction() {
	columnFamilyEngines.values().forEach(engine -> engine.runCompaction());
    }

    @Override
    public void close() {
	logger.info("Closing table engine '" + tableDescriptor.getName() + "'...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	for (ColumnFamilyImpl columnFamilyEngine : columnFamilyEngines.values()) {
	    columnFamilyEngine.close();
	}
	stopWatch.stop();
	logger.info("Table engine '" + tableDescriptor.getName() + "' closed in " + stopWatch.getMillis() + "ms.");
    }

    @Override
    public Set<Key> getColumnFamilies() {
	return columnFamilyEngines.keySet();
    }

    public ColumnFamily getColumnFamilyEngine(Key columnFamily) {
	return columnFamilyEngines.get(columnFamily);
    }

    @Override
    public void put(Put put) {
	for (Key columnFamily : put.getColumnFamilies()) {
	    ColumnFamilyImpl columnFamilyEngine = columnFamilyEngines.get(columnFamily);
	    columnFamilyEngine.put(put.getKey(), put.getColumnValues(columnFamily));
	}
    }

    @Override
    public void put(List<Put> puts) {
	for (Put put : puts) {
	    put(put);
	}
    }

    @Override
    public void delete(Delete delete) {
	Key rowKey = delete.getKey();
	Set<Key> columnFamilies = delete.getColumnFamilies();
	if (!columnFamilies.isEmpty()) {
	    for (Key columnFamily : columnFamilies) {
		Set<Key> columns = delete.getColumns(columnFamily);
		ColumnFamilyImpl columnFamilyEngine = columnFamilyEngines.get(columnFamily);
		if (columns.size() == 0) {
		    columnFamilyEngine.delete(rowKey);
		} else {
		    columnFamilyEngine.delete(rowKey, columns);
		}
	    }
	} else {
	    for (Entry<Key, ColumnFamilyImpl> columnFamily : columnFamilyEngines.entrySet()) {
		ColumnFamilyImpl columnFamilyEngine = columnFamily.getValue();
		columnFamilyEngine.delete(rowKey);
	    }
	}
    }

    @Override
    public void delete(List<Delete> deletes) {
	for (Delete delete : deletes) {
	    delete(delete);
	}
    }

    @Override
    public Result get(Get get) {
	Key rowKey = get.getKey();
	Result result = new Result(rowKey);
	NavigableMap<Key, NavigableSet<Key>> columnFamilies = get.getColumnFamilies();
	if (!columnFamilies.isEmpty()) {
	    for (Key columnFamily : columnFamilies.keySet()) {
		ColumnMap columns = columnFamilyEngines.get(columnFamily).get(rowKey);
		result.add(columnFamily, columns);
	    }
	} else {
	    for (Entry<Key, ColumnFamilyImpl> columnFamily : columnFamilyEngines.entrySet()) {
		ColumnMap columns = columnFamily.getValue().get(rowKey);
		result.add(columnFamily.getKey(), columns);
	    }
	}
	return result;
    }

    public Set<ColumnFamily> getColumnFamilyEngines() {
	Set<ColumnFamily> columnFamilies = new HashSet<>();
	for (Key columnFamilyName : getColumnFamilies()) {
	    columnFamilies.add(getColumnFamilyEngine(columnFamilyName));
	}
	return columnFamilies;
    }

    @Override
    public ResultScanner getScanner(Scan scan) {
	try {
	    return new ResultScanner(this, scan);
	} catch (StorageException e) {
	    logger.error("Could not create result scanner.", e);
	    return null;
	}
    }

    @Override
    public ResultScanner find(Scan scan, Key columnKey, ColumnValue value) {
	try {
	    return new ResultScanner(this, scan, columnKey, value);
	} catch (StorageException e) {
	    logger.error("Could not create result scanner.", e);
	    return null;
	}
    }

    @Override
    public ResultScanner find(Scan scan, Key columnKey, ColumnValue fromValue, ColumnValue toValue) {
	try {
	    return new ResultScanner(this, scan, columnKey, fromValue, toValue);
	} catch (StorageException e) {
	    logger.error("Could not create result scanner.", e);
	    return null;
	}
    }

    @Override
    public long incrementColumnValue(Key rowKey, Key columnFamily, Key column, long incrementValue) {
	ColumnFamily columnFamilyEngine = getColumnFamilyEngine(columnFamily);
	return columnFamilyEngine.incrementColumnValue(rowKey, column, incrementValue);
    }

}
