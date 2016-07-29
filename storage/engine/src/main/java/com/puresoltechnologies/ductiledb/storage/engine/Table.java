package com.puresoltechnologies.ductiledb.storage.engine;

import java.util.List;

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

    private final TableEngineImpl tableEngine;
    private final TableDescriptor tableDescriptor;

    public Table(TableEngineImpl tableEngine, TableDescriptor tableDescriptor) {
	super();
	this.tableEngine = tableEngine;
	this.tableDescriptor = tableDescriptor;
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
	// TODO Auto-generated method stub
	return null;
    }

    public long incrementColumnValue(byte[] rowKey, String tableName, byte[] key, long incrementValue) {
	// TODO Auto-generated method stub
	return 0;
    }

    public ColumnFamily getColumnFamily(ColumnFamilyDescriptor columnFamilyDescriptor) {
	return new ColumnFamily(tableEngine.getColumnFamilyEngine(columnFamilyDescriptor));
    }

}
