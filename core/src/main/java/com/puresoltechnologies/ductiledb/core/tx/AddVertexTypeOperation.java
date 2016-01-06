package com.puresoltechnologies.ductiledb.core.tx;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.core.schema.HBaseColumnFamily;
import com.puresoltechnologies.ductiledb.core.schema.HBaseTable;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

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
	Put put = new Put(id);
	put.addColumn(HBaseColumnFamily.TYPES.getNameBytes(), Bytes.toBytes(type), Bytes.toBytes(type));
	Put index = OperationsHelper.createVertexTypeIndexPut(vertexId, type);
	put(HBaseTable.VERTICES.getTableName(), put);
	put(HBaseTable.VERTEX_TYPES.getTableName(), index);
    }
}
