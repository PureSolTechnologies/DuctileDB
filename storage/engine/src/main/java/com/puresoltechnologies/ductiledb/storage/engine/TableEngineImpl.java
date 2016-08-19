package com.puresoltechnologies.ductiledb.storage.engine;

import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.commons.misc.StopWatch;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.TableDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.utils.ByteArrayComparator;
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
    private final TreeMap<byte[], ColumnFamilyEngineImpl> columnFamilyEngines = new TreeMap<>(
	    ByteArrayComparator.getInstance());

    public TableEngineImpl(Storage storage, TableDescriptor tableDescriptor, DatabaseEngineConfiguration configuration)
	    throws StorageException {
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

    private void initializeColumnFamilyEngines() throws StorageException {
	for (ColumnFamilyDescriptor columnFamilyDescriptor : tableDescriptor.getColumnFamilies()) {
	    addColumnFamily(columnFamilyDescriptor);
	}
    }

    public void addColumnFamily(ColumnFamilyDescriptor columnFamilyDescriptor) throws StorageException {
	columnFamilyEngines.put(columnFamilyDescriptor.getName(),
		new ColumnFamilyEngineImpl(storage, columnFamilyDescriptor, configuration));
    }

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

    public Set<byte[]> getColumnFamilies() {
	return columnFamilyEngines.keySet();
    }

    public ColumnFamilyEngineImpl getColumnFamilyEngine(byte[] columnFamily) {
	return columnFamilyEngines.get(columnFamily);
    }

    public void put(Put put) throws StorageException {
	for (byte[] columnFamily : put.getColumnFamilies()) {
	    ColumnFamilyEngineImpl columnFamilyEngine = columnFamilyEngines.get(columnFamily);
	    columnFamilyEngine.put(put.getKey(), put.getColumnValues(columnFamily));
	}
    }

    public void delete(Delete delete) throws StorageException {
	byte[] rowKey = delete.getKey();
	Set<byte[]> columnFamilies = delete.getColumnFamilies();
	if (!columnFamilies.isEmpty()) {
	    for (byte[] columnFamily : columnFamilies) {
		Set<byte[]> columns = delete.getColumns(columnFamily);
		ColumnFamilyEngineImpl columnFamilyEngine = columnFamilyEngines.get(columnFamily);
		if (columns.size() == 0) {
		    columnFamilyEngine.delete(rowKey);
		} else {
		    columnFamilyEngine.delete(rowKey, columns);
		}
	    }
	} else {
	    for (Entry<byte[], ColumnFamilyEngineImpl> columnFamily : columnFamilyEngines.entrySet()) {
		ColumnFamilyEngineImpl columnFamilyEngine = columnFamily.getValue();
		columnFamilyEngine.delete(rowKey);
	    }
	}
    }

    public Result get(Get get) throws StorageException {
	byte[] rowKey = get.getKey();
	Result result = new Result(rowKey);
	NavigableMap<byte[], NavigableSet<byte[]>> columnFamilies = get.getColumnFamilies();
	if (!columnFamilies.isEmpty()) {
	    for (byte[] columnFamily : columnFamilies.keySet()) {
		ColumnMap columns = columnFamilyEngines.get(columnFamily).get(rowKey);
		result.add(columnFamily, columns);
	    }
	} else {
	    for (Entry<byte[], ColumnFamilyEngineImpl> columnFamily : columnFamilyEngines.entrySet()) {
		ColumnMap columns = columnFamily.getValue().get(rowKey);
		result.add(columnFamily.getKey(), columns);
	    }
	}
	return result;
    }
}
