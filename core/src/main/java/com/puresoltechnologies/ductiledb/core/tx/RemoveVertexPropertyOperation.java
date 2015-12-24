package com.puresoltechnologies.ductiledb.core.tx;

import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.PROPERTIES_COLUMN_FAMILY_BYTES;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.DuctileDBDetachedVertexImpl;
import com.puresoltechnologies.ductiledb.core.schema.SchemaTable;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

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
	DuctileDBTransactionImpl transaction = getTransaction();
	DuctileDBDetachedVertexImpl vertex = (DuctileDBDetachedVertexImpl) transaction.getVertex(vertexId);
	vertex.removePropertyInternally(key);
    }

    @Override
    public void rollbackInternally() {
	if (oldValue != null) {
	    DuctileDBTransactionImpl transaction = getTransaction();
	    DuctileDBDetachedVertexImpl vertex = (DuctileDBDetachedVertexImpl) transaction.getVertex(vertexId);
	    vertex.setPropertyInternally(key, oldValue);
	}
    }

    @Override
    public void perform() throws IOException {
	byte[] id = IdEncoder.encodeRowId(vertexId);
	Delete delete = new Delete(id);
	delete.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, Bytes.toBytes(key));
	Delete index = OperationsHelper.createVertexPropertyIndexDelete(vertexId, key);
	delete(SchemaTable.VERTICES.getTableName(), delete);
	delete(SchemaTable.VERTEX_PROPERTIES.getTableName(), index);
    }
}
