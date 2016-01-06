package com.puresoltechnologies.ductiledb.core.tx;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.schema.HBaseColumnFamily;
import com.puresoltechnologies.ductiledb.core.schema.HBaseTable;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

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
	Delete delete = new Delete(id);
	delete.addColumn(HBaseColumnFamily.TYPES.getNameBytes(), Bytes.toBytes(type));
	Delete index = OperationsHelper.createVertexTypeIndexDelete(vertexId, type);
	delete(HBaseTable.VERTICES.getTableName(), delete);
	delete(HBaseTable.VERTEX_TYPES.getTableName(), index);
    }
}
