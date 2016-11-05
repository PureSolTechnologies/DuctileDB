package com.puresoltechnologies.ductiledb.core.graph.tx;

import java.io.IOException;

import com.puresoltechnologies.ductiledb.core.graph.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseColumnFamily;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseTable;
import com.puresoltechnologies.ductiledb.core.graph.utils.IdEncoder;
import com.puresoltechnologies.ductiledb.storage.engine.Delete;
import com.puresoltechnologies.ductiledb.storage.engine.Key;

public class RemoveVertexTypeOperation extends AbstractTxOperation {

    private final long vertexId;
    private final String type;
    private final boolean wasPresent;

    public RemoveVertexTypeOperation(DuctileDBTransactionImpl transaction, DuctileDBVertex vertex, String type) {
	super(transaction);
	this.vertexId = vertex.getId();
	this.type = type;
	this.wasPresent = vertex.hasType(type);
    }

    @Override
    public void commitInternally() {
	DuctileDBCacheVertex vertex = getTransaction().getCachedVertex(vertexId);
	vertex.removeType(type);
    }

    @Override
    public void rollbackInternally() {
	if (!wasPresent) {
	    DuctileDBCacheVertex vertex = getTransaction().getCachedVertex(vertexId);
	    vertex.addType(type);
	}
    }

    @Override
    public void perform() throws IOException {
	byte[] id = IdEncoder.encodeRowId(vertexId);
	Delete delete = new Delete(Key.of(id));
	delete.addColumns(DatabaseColumnFamily.TYPES.getKey(), Key.of(type));
	Delete index = OperationsHelper.createVertexTypeIndexDelete(vertexId, type);
	delete(DatabaseTable.VERTICES.getName(), delete);
	delete(DatabaseTable.VERTEX_TYPES.getName(), index);
    }
}
