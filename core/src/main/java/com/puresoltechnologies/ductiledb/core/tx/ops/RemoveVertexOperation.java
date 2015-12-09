package com.puresoltechnologies.ductiledb.core.tx.ops;

import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.VERTICES_TABLE;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;

import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

public class RemoveVertexOperation extends AbstractTxOperation {

    private final long vertexId;

    public RemoveVertexOperation(Connection connection, long vertexId) {
	super(connection);
	this.vertexId = vertexId;
    }

    @Override
    public void perform() throws IOException {
	byte[] id = IdEncoder.encodeRowId(vertexId);
	Delete delete = new Delete(id);
	delete(VERTICES_TABLE, delete);
    }

}
