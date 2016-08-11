package com.puresoltechnologies.ductiledb.storage.engine;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.schema.ColumnFamilyDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.TableDescriptor;

/**
 * This class is used to access a single table within the
 * {@link DatabaseEngine}.
 * 
 * @author Rick-Rainer Ludwig
 */
public class Table {

    private static final Logger logger = LoggerFactory.getLogger(Table.class);

    private final TableEngineImpl tableEngine;
    private final TableDescriptor tableDescriptor;

    public Table(TableEngineImpl tableEngine, TableDescriptor tableDescriptor) {
	super();
	this.tableEngine = tableEngine;
	this.tableDescriptor = tableDescriptor;
    }

    public TableDescriptor getTableDescriptor() {
	return tableDescriptor;
    }

    public void put(Put put) throws StorageException {
	tableEngine.put(put);
    }

    public void put(List<Put> puts) throws StorageException {
	for (Put put : puts) {
	    put(put);
	}
    }

    public void delete(Delete delete) throws StorageException {
	tableEngine.delete(delete);
    }

    public void delete(List<Delete> deletes) throws StorageException {
	for (Delete delete : deletes) {
	    delete(delete);
	}
    }

    public Result get(Get get) throws StorageException {
	return tableEngine.get(get);
    }

    public ResultScanner getScanner(Scan scan) {
	try {
	    return new ResultScanner(this, scan);
	} catch (StorageException e) {
	    logger.error("Could not create result scanner.", e);
	    return null;
	}
    }

    public long incrementColumnValue(byte[] rowKey, byte[] columnFamily, byte[] column, long incrementValue)
	    throws StorageException {
	ColumnFamilyEngineImpl columnFamilyEngine = tableEngine.getColumnFamilyEngine(columnFamily);
	columnFamilyEngine.incrementColumnValue(rowKey, column, incrementValue);
	return 0;
    }

    public ColumnFamily getColumnFamily(ColumnFamilyDescriptor columnFamilyDescriptor) {
	return new ColumnFamily(tableEngine.getColumnFamilyEngine(columnFamilyDescriptor.getName()));
    }

    public ColumnFamily getColumnFamily(byte[] columnFamily) {
	return new ColumnFamily(tableEngine.getColumnFamilyEngine(columnFamily));
    }

}
