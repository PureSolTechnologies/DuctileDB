package com.puresoltechnologies.ductiledb.engine;

import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.commons.misc.StopWatch;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnFamilyEngine;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnFamilyEngineImpl;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnMap;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnValue;
import com.puresoltechnologies.ductiledb.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.engine.schema.TableDescriptor;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This class is the central engine class for table storage. It is using the
 * {@link ColumnFamilyEngine} to store the separated column families.
 * 
 * @author Rick-Rainer Ludwig
 */
public class TableEngineImpl implements TableEngine {

    private static final Logger logger = LoggerFactory.getLogger(TableEngine.class);

    private final Storage storage;
    private final TableDescriptor tableDescriptor;
    private final DatabaseEngineConfiguration configuration;
    private final TreeMap<Key, ColumnFamilyEngineImpl> columnFamilyEngines = new TreeMap<>();

    public TableEngineImpl(Storage storage, TableDescriptor tableDescriptor,
	    DatabaseEngineConfiguration configuration) {
	super();
	this.storage = storage;
	this.tableDescriptor = tableDescriptor;
	this.configuration = configuration;
	logger.info("Starting table engine '" + tableDescriptor.getName() + "'...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	initializeColumnFamilyEngines();
	stopWatch.stop();
	logger.info("Table engine '" + tableDescriptor.getName() + "' started in " + stopWatch.getMillis() + "ms.");
    }

    private void initializeColumnFamilyEngines() {
	for (ColumnFamilyDescriptor columnFamilyDescriptor : tableDescriptor.getColumnFamilies()) {
	    addColumnFamily(columnFamilyDescriptor);
	}
    }

    @Override
    public void addColumnFamily(ColumnFamilyDescriptor columnFamilyDescriptor) {
	columnFamilyEngines.put(columnFamilyDescriptor.getName(),
		new ColumnFamilyEngineImpl(storage, columnFamilyDescriptor, configuration));
    }

    @Override
    public void dropColumnFamily(ColumnFamilyDescriptor columnFamilyDescriptor) {
	ColumnFamilyEngineImpl columnFamilyEngine = columnFamilyEngines.get(tableDescriptor.getName());
	if (columnFamilyEngine != null) {
	    columnFamilyEngines.remove(columnFamilyDescriptor.getName());
	    columnFamilyEngine.close();
	}
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
	for (ColumnFamilyEngineImpl columnFamilyEngine : columnFamilyEngines.values()) {
	    columnFamilyEngine.close();
	}
	stopWatch.stop();
	logger.info("Table engine '" + tableDescriptor.getName() + "' closed in " + stopWatch.getMillis() + "ms.");
    }

    @Override
    public Set<Key> getColumnFamilies() {
	return columnFamilyEngines.keySet();
    }

    public ColumnFamilyEngineImpl getColumnFamilyEngine(Key columnFamily) {
	return columnFamilyEngines.get(columnFamily);
    }

    @Override
    public void put(Put put) {
	for (Key columnFamily : put.getColumnFamilies()) {
	    ColumnFamilyEngineImpl columnFamilyEngine = columnFamilyEngines.get(columnFamily);
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
		ColumnFamilyEngineImpl columnFamilyEngine = columnFamilyEngines.get(columnFamily);
		if (columns.size() == 0) {
		    columnFamilyEngine.delete(rowKey);
		} else {
		    columnFamilyEngine.delete(rowKey, columns);
		}
	    }
	} else {
	    for (Entry<Key, ColumnFamilyEngineImpl> columnFamily : columnFamilyEngines.entrySet()) {
		ColumnFamilyEngineImpl columnFamilyEngine = columnFamily.getValue();
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
	    for (Entry<Key, ColumnFamilyEngineImpl> columnFamily : columnFamilyEngines.entrySet()) {
		ColumnMap columns = columnFamily.getValue().get(rowKey);
		result.add(columnFamily.getKey(), columns);
	    }
	}
	return result;
    }

    public Set<ColumnFamilyEngine> getColumnFamilyEngines() {
	Set<ColumnFamilyEngine> columnFamilies = new HashSet<>();
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
	ColumnFamilyEngineImpl columnFamilyEngine = getColumnFamilyEngine(columnFamily);
	return columnFamilyEngine.incrementColumnValue(rowKey, column, incrementValue);
    }

}
