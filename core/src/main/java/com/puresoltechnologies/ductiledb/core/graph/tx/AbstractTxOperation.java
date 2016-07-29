package com.puresoltechnologies.ductiledb.core.graph.tx;

import java.io.IOException;
import java.util.List;

import com.puresoltechnologies.ductiledb.api.graph.manager.DuctileDBGraphManagerException;
import com.puresoltechnologies.ductiledb.core.graph.schema.GraphSchema;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngine;
import com.puresoltechnologies.ductiledb.storage.engine.Delete;
import com.puresoltechnologies.ductiledb.storage.engine.Put;
import com.puresoltechnologies.ductiledb.storage.engine.Table;

public abstract class AbstractTxOperation implements TxOperation {

    private final DuctileDBTransactionImpl transaction;
    private final DatabaseEngine storageEngine;

    public AbstractTxOperation(DuctileDBTransactionImpl transaction) {
	super();
	this.transaction = transaction;
	this.storageEngine = transaction.getStorageEngine();
    }

    public final DuctileDBTransactionImpl getTransaction() {
	return transaction;
    }

    public final DatabaseEngine getStorageEngine() {
	return storageEngine;
    }

    protected void put(String tableName, Put put) throws IOException {
	try {
	    Table table = storageEngine.getTable(GraphSchema.DUCTILEDB_NAMESPACE, tableName);
	    table.put(put);
	} catch (StorageException e) {
	    throw new DuctileDBGraphManagerException("Could not put to table.", e);
	}
    }

    protected void put(String tableName, List<Put> puts) throws IOException {
	if (puts.isEmpty()) {
	    return;
	}
	try {
	    Table table = storageEngine.getTable(GraphSchema.DUCTILEDB_NAMESPACE, tableName);
	    table.put(puts);
	} catch (StorageException e) {
	    throw new DuctileDBGraphManagerException("Could not put to table.", e);
	}
    }

    protected void delete(String tableName, Delete delete) throws IOException {
	try {
	    Table table = storageEngine.getTable(GraphSchema.DUCTILEDB_NAMESPACE, tableName);
	    table.delete(delete);
	} catch (StorageException e) {
	    throw new DuctileDBGraphManagerException("Could not delete from table.", e);
	}
    }

    protected void delete(String tableName, List<Delete> deletes) throws IOException {
	if (deletes.isEmpty()) {
	    return;
	}
	try {
	    Table table = storageEngine.getTable(GraphSchema.DUCTILEDB_NAMESPACE, tableName);
	    table.delete(deletes);
	} catch (StorageException e) {
	    throw new DuctileDBGraphManagerException("Could not delete from table.", e);
	}
    }

}
