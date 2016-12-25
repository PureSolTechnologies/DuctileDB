package com.puresoltechnologies.ductiledb.core.graph.tx;

import java.io.IOException;

import com.puresoltechnologies.ductiledb.core.graph.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseColumnFamily;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseTable;
import com.puresoltechnologies.ductiledb.core.graph.utils.IdEncoder;
import com.puresoltechnologies.ductiledb.engine.Delete;
import com.puresoltechnologies.ductiledb.logstore.Key;

public class RemoveVertexPropertyOperation extends AbstractTxOperation {

    private final long vertexId;
    private final String key;
    private final Object oldValue;

    public RemoveVertexPropertyOperation(DuctileDBTransactionImpl transaction, DuctileDBVertex vertex, String key) {
	super(transaction);
	this.vertexId = vertex.getId();
	this.key = key;
	this.oldValue = vertex.getProperty(key);
    }

    @Override
    public void commitInternally() {
	DuctileDBCacheVertex vertex = getTransaction().getCachedVertex(vertexId);
	vertex.removeProperty(key);
    }

    @Override
    public void rollbackInternally() {
	if (oldValue != null) {
	    DuctileDBCacheVertex vertex = getTransaction().getCachedVertex(vertexId);
	    vertex.setProperty(key, oldValue);
	}
    }

    @Override
    public void perform() throws IOException {
	byte[] id = IdEncoder.encodeRowId(vertexId);
	Delete delete = new Delete(Key.of(id));
	delete.addColumns(DatabaseColumnFamily.PROPERTIES.getKey(), Key.of(key));
	Delete index = OperationsHelper.createVertexPropertyIndexDelete(vertexId, key);
	delete(DatabaseTable.VERTICES.getName(), delete);
	delete(DatabaseTable.VERTEX_PROPERTIES.getName(), index);
    }
}
