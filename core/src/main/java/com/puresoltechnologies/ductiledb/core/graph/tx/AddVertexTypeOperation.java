package com.puresoltechnologies.ductiledb.core.graph.tx;

import java.io.IOException;

import com.puresoltechnologies.ductiledb.bigtable.Put;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnValue;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseColumnFamily;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseTable;
import com.puresoltechnologies.ductiledb.core.graph.utils.IdEncoder;
import com.puresoltechnologies.ductiledb.logstore.Key;

public class AddVertexTypeOperation extends AbstractTxOperation {

    private final long vertexId;
    private final String type;
    private final boolean wasPresent;

    public AddVertexTypeOperation(DuctileDBTransactionImpl transaction, long vertexId, String type) {
	super(transaction);
	this.vertexId = vertexId;
	this.type = type;
	this.wasPresent = getTransaction().getVertex(vertexId).hasType(type);
    }

    @Override
    public void commitInternally() {
	DuctileDBCacheVertex vertex = getTransaction().getCachedVertex(vertexId);
	vertex.addType(type);
    }

    @Override
    public void rollbackInternally() {
	if (!wasPresent) {
	    DuctileDBCacheVertex vertex = getTransaction().getCachedVertex(vertexId);
	    vertex.removeType(type);
	}
    }

    @Override
    public void perform() throws IOException {
	byte[] id = IdEncoder.encodeRowId(vertexId);
	Put put = new Put(Key.of(id));
	put.addColumn(DatabaseColumnFamily.TYPES.getKey(), Key.of(type), ColumnValue.of(type));
	Put index = OperationsHelper.createVertexTypeIndexPut(vertexId, type);
	put(DatabaseTable.VERTICES.getName(), put);
	put(DatabaseTable.VERTEX_TYPES.getName(), index);
    }
}
