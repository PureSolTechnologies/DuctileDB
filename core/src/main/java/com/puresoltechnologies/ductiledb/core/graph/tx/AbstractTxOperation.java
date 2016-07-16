package com.puresoltechnologies.ductiledb.core.graph.tx;

import java.io.IOException;
import java.util.List;

import com.puresoltechnologies.ductiledb.storage.engine.Delete;
import com.puresoltechnologies.ductiledb.storage.engine.Put;
import com.puresoltechnologies.ductiledb.storage.engine.StorageEngine;
import com.puresoltechnologies.ductiledb.storage.engine.Table;

public abstract class AbstractTxOperation implements TxOperation {

    private final DuctileDBTransactionImpl transaction;
    private final StorageEngine storageEngine;

    public AbstractTxOperation(DuctileDBTransactionImpl transaction) {
	super();
	this.transaction = transaction;
	this.storageEngine = transaction.getStorageEngine();
    }

    public final DuctileDBTransactionImpl getTransaction() {
	return transaction;
    }

    public final StorageEngine getStorageEngine() {
	return storageEngine;
    }

    protected void put(String tableName, Put put) throws IOException {
	try (Table table = storageEngine.getTable(tableName)) {
	    table.put(put);
	}
    }

    protected void put(String tableName, List<Put> puts) throws IOException {
	if (puts.isEmpty()) {
	    return;
	}
	try (Table table = storageEngine.getTable(tableName)) {
	    table.put(puts);
	}
    }

    protected void delete(String tableName, Delete delete) throws IOException {
	try (Table table = storageEngine.getTable(tableName)) {
	    table.delete(delete);
	}
    }

    protected void delete(String tableName, List<Delete> deletes) throws IOException {
	if (deletes.isEmpty()) {
	    return;
	}
	try (Table table = storageEngine.getTable(tableName)) {
	    table.delete(deletes);
	}
    }

}
