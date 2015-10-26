package com.puresoltechnologies.hgraph.tx;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;

public class HGraphOperation {

    private final OperationType operationType;
    private final Put put;
    private final Delete delete;

    public HGraphOperation(Put put) {
	super();
	this.operationType = OperationType.PUT;
	this.put = put;
	this.delete = null;
    }

    public HGraphOperation(Delete delete) {
	super();
	this.operationType = OperationType.DELETE;
	this.put = null;
	this.delete = delete;
    }

    public OperationType getOperationType() {
	return operationType;
    }

    public Put getPut() {
	return put;
    }

    public Delete getDelete() {
	return delete;
    }

}
