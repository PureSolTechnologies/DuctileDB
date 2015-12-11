package com.puresoltechnologies.ductiledb.core.tx;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

public abstract class AbstractTxOperation implements TxOperation {

    private final DuctileDBTransactionImpl transaction;
    private final Connection connection;

    public AbstractTxOperation(DuctileDBTransactionImpl transaction) {
	super();
	this.transaction = transaction;
	this.connection = transaction.getConnection();
    }

    public final DuctileDBTransactionImpl getTransaction() {
	return transaction;
    }

    public final Connection getConnection() {
	return connection;
    }

    protected void put(String tableName, Put put) throws IOException {
	try (Table table = connection.getTable(TableName.valueOf(tableName))) {
	    table.put(put);
	}
    }

    protected void put(String tableName, List<Put> puts) throws IOException {
	if (puts.isEmpty()) {
	    return;
	}
	try (Table table = connection.getTable(TableName.valueOf(tableName))) {
	    table.put(puts);
	}
    }

    protected void delete(String tableName, Delete delete) throws IOException {
	try (Table table = connection.getTable(TableName.valueOf(tableName))) {
	    table.delete(delete);
	}
    }

    protected void delete(String tableName, List<Delete> deletes) throws IOException {
	if (deletes.isEmpty()) {
	    return;
	}
	try (Table table = connection.getTable(TableName.valueOf(tableName))) {
	    table.delete(deletes);
	}
    }

}
