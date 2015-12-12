package com.puresoltechnologies.ductiledb.core.tx;

import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.VERTICES_TABLE;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Delete;

import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

public class RemoveVertexOperation extends AbstractTxOperation {

    private final long vertexId;

    public RemoveVertexOperation(DuctileDBTransactionImpl transaction, long vertexId) {
	super(transaction);
	this.vertexId = vertexId;
	transaction.removeCachedVertex(vertexId);
    }

    public long getVertexId() {
	return vertexId;
    }

    @Override
    public void perform() throws IOException {
	byte[] id = IdEncoder.encodeRowId(vertexId);
	Delete delete = new Delete(id);
	delete(VERTICES_TABLE, delete);
    }
}