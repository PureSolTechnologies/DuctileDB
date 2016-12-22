package com.puresoltechnologies.ductiledb.core.graph.tx;

import java.io.IOException;
import java.util.List;

import com.puresoltechnologies.ductiledb.core.graph.manager.DuctileDBGraphManagerException;
import com.puresoltechnologies.ductiledb.engine.DatabaseEngine;
import com.puresoltechnologies.ductiledb.engine.Delete;
import com.puresoltechnologies.ductiledb.engine.Put;
import com.puresoltechnologies.ductiledb.engine.TableEngine;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;

public abstract class AbstractTxOperation implements TxOperation {

    private final DuctileDBTransactionImpl transaction;
    private final DatabaseEngine storageEngine;
    private final String namespace;

    public AbstractTxOperation(DuctileDBTransactionImpl transaction) {
	super();
	this.transaction = transaction;
	this.storageEngine = transaction.getStorageEngine();
	this.namespace = transaction.getNamespace();
    }

    public final DuctileDBTransactionImpl getTransaction() {
	return transaction;
    }

    public final DatabaseEngine getStorageEngine() {
	return storageEngine;
    }

    public final String getNamespace() {
	return namespace;
    }

    protected void put(String tableName, Put put) throws IOException {
	try {
	    TableEngine table = storageEngine.getTable(namespace, tableName);
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
	    TableEngine table = storageEngine.getTable(namespace, tableName);
	    table.put(puts);
	} catch (StorageException e) {
	    throw new DuctileDBGraphManagerException("Could not put to table.", e);
	}
    }

    protected void delete(String tableName, Delete delete) throws IOException {
	try {
	    TableEngine table = storageEngine.getTable(namespace, tableName);
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
	    TableEngine table = storageEngine.getTable(namespace, tableName);
	    table.delete(deletes);
	} catch (StorageException e) {
	    throw new DuctileDBGraphManagerException("Could not delete from table.", e);
	}
    }

}
