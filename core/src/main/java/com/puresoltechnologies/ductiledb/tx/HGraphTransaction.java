package com.puresoltechnologies.ductiledb.tx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import com.puresoltechnologies.ductiledb.HGraphException;

/**
 * This transaction is used per thread to record changes in the graph to be
 * commited as batch.
 * 
 * @author Rick-Rainer Ludwig
 */
public class HGraphTransaction {

    public final Map<TableName, List<HGraphOperation>> tableOperations = new HashMap<>();

    public void put(String tableName, Put put) {
	TableName table = TableName.valueOf(tableName);
	List<HGraphOperation> operationList = tableOperations.get(table);
	if (operationList == null) {
	    operationList = new ArrayList<>();
	    tableOperations.put(table, operationList);
	}
	operationList.add(new HGraphOperation(put));
    }

    public void delete(String tableName, Delete delete) {
	TableName table = TableName.valueOf(tableName);
	List<HGraphOperation> operationList = tableOperations.get(table);
	if (operationList == null) {
	    operationList = new ArrayList<>();
	    tableOperations.put(table, operationList);
	}
	operationList.add(new HGraphOperation(delete));
    }

    public void commit(Connection connection) {
	try {
	    for (Entry<TableName, List<HGraphOperation>> entry : tableOperations.entrySet()) {
		mutateTable(connection, entry.getKey(), entry.getValue());
	    }
	    tableOperations.clear();
	} catch (IOException e) {
	    throw new HGraphException("Could not cmomit changes.", e);
	}
    }

    private void mutateTable(Connection connection, TableName tableName, List<HGraphOperation> operations)
	    throws IOException {
	try (Table table = connection.getTable(tableName)) {
	    List<Put> puts = new ArrayList<>();
	    List<Delete> deletes = new ArrayList<>();
	    OperationType currentOperationType = null;
	    for (HGraphOperation operation : operations) {
		if (currentOperationType != operation.getOperationType()) {
		    if (currentOperationType != null) {
			switch (currentOperationType) {
			case PUT:
			    table.put(puts);
			    puts.clear();
			    break;
			case DELETE:
			    table.delete(deletes);
			    deletes.clear();
			    break;
			default:
			    throw new HGraphException(
				    "Operation type '" + currentOperationType + "' is not implemented.");
			}
		    }
		    currentOperationType = operation.getOperationType();
		}
		switch (currentOperationType) {
		case PUT:
		    puts.add(operation.getPut());
		    break;
		case DELETE:
		    deletes.add(operation.getDelete());
		    break;
		default:
		    throw new HGraphException("Operation type '" + currentOperationType + "' is not implemented.");
		}
	    }
	    switch (currentOperationType) {
	    case PUT:
		table.put(puts);
		break;
	    case DELETE:
		table.delete(deletes);
		break;
	    default:
		throw new HGraphException("Operation type '" + currentOperationType + "' is not implemented.");
	    }
	}
    }

    public void rollback() {
	tableOperations.clear();
    }

}
