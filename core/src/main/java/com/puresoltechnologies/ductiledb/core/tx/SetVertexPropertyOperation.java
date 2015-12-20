package com.puresoltechnologies.ductiledb.core.tx;

import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.PROPERTIES_COLUMN_FAMILY_BYTES;

import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.DuctileDBVertexImpl;
import com.puresoltechnologies.ductiledb.core.schema.SchemaTable;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

public class SetVertexPropertyOperation extends AbstractTxOperation {

    private final long vertexId;
    private final String key;
    private final Object value;
    private final Object oldValue;

    public SetVertexPropertyOperation(DuctileDBTransactionImpl transaction, DuctileDBVertex vertex, String key,
	    Object value) {
	super(transaction);
	this.vertexId = vertex.getId();
	this.key = key;
	this.value = value;
	this.oldValue = vertex.getProperty(key);
    }

    @Override
    public void commitInternally() {
	DuctileDBTransactionImpl transaction = getTransaction();
	DuctileDBVertexImpl vertex = (DuctileDBVertexImpl) transaction.getVertex(vertexId);
	vertex.setPropertyInternally(key, value);
    }

    @Override
    public void rollbackInternally() {
	DuctileDBTransactionImpl transaction = getTransaction();
	DuctileDBVertexImpl vertex = (DuctileDBVertexImpl) transaction.getVertex(vertexId);
	if (oldValue == null) {
	    vertex.removePropertyInternally(key);
	} else {
	    vertex.setPropertyInternally(key, oldValue);
	}
    }

    @Override
    public void perform() throws IOException {
	byte[] id = IdEncoder.encodeRowId(vertexId);
	Put put = new Put(id);
	put.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, Bytes.toBytes(key),
		SerializationUtils.serialize((Serializable) value));
	Put index = OperationsHelper.createVertexPropertyIndexPut(vertexId, key, value);
	put(SchemaTable.VERTICES.getTableName(), put);
	put(SchemaTable.VERTEX_PROPERTIES.getTableName(), index);
    }
}
