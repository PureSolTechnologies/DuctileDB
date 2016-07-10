package com.puresoltechnologies.ductiledb.core.graph.tx;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Delete;

import com.puresoltechnologies.ductiledb.core.graph.schema.HBaseTable;
import com.puresoltechnologies.ductiledb.core.graph.utils.IdEncoder;

public class RemoveVertexOperation extends AbstractTxOperation {

    private final long vertexId;

    public RemoveVertexOperation(DuctileDBTransactionImpl transaction, long vertexId) {
	super(transaction);
	this.vertexId = vertexId;
    }

    @Override
    public void commitInternally() {
	DuctileDBTransactionImpl transaction = getTransaction();
	transaction.removeCachedVertex(vertexId);
    }

    @Override
    public void rollbackInternally() {
	// Intentionally left empty...
    }

    @Override
    public void perform() throws IOException {
	byte[] id = IdEncoder.encodeRowId(vertexId);
	Delete delete = new Delete(id);
	delete(HBaseTable.VERTICES.getTableName(), delete);
    }
}
