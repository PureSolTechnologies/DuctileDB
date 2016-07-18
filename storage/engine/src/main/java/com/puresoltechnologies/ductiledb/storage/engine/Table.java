package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.Closeable;
import java.util.List;

import com.puresoltechnologies.ductiledb.storage.engine.schema.TableDescriptor;

/**
 * This class is used to access a single table within the {@link StorageEngine}.
 * 
 * @author Rick-Rainer Ludwig
 */
public class Table implements Closeable {

    private final StorageEngine storageEngine;
    private final TableDescriptor tableDescriptor;

    public Table(StorageEngine storageEngine, TableDescriptor tableDescriptor) {
	super();
	this.storageEngine = storageEngine;
	this.tableDescriptor = tableDescriptor;
    }

    @Override
    public void close() {
	// TODO Auto-generated method stub
    }

    public void put(Put put) {
	storageEngine.store(tableDescriptor, put);
    }

    public void put(List<Put> puts) {
	for (Put put : puts) {
	    put(put);
	}
    }

    public void delete(Delete delete) {
	storageEngine.delete(tableDescriptor, delete);
    }

    public void delete(List<Delete> deletes) {
	for (Delete delete : deletes) {
	    delete(delete);
	}
    }

    public Result get(Get get) {
	// TODO Auto-generated method stub
	return null;
    }

    public ResultScanner getScanner(Scan scan) {
	// TODO Auto-generated method stub
	return null;
    }

    public long incrementColumnValue(byte[] rowKey, String tableName, byte[] key, long incrementValue) {
	// TODO Auto-generated method stub
	return 0;
    }

}
