package com.puresoltechnologies.ductiledb.core.graph.tx;

import java.io.IOException;

import com.puresoltechnologies.ductiledb.api.graph.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.graph.schema.HBaseColumnFamily;
import com.puresoltechnologies.ductiledb.core.graph.schema.HBaseTable;
import com.puresoltechnologies.ductiledb.core.graph.utils.IdEncoder;
import com.puresoltechnologies.ductiledb.storage.engine.Delete;
import com.puresoltechnologies.ductiledb.storage.engine.utils.Bytes;

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
	Delete delete = new Delete(id);
	delete.addColumns(HBaseColumnFamily.PROPERTIES.getNameBytes(), Bytes.toBytes(key));
	Delete index = OperationsHelper.createVertexPropertyIndexDelete(vertexId, key);
	delete(HBaseTable.VERTICES.getName(), delete);
	delete(HBaseTable.VERTEX_PROPERTIES.getName(), index);
    }
}
