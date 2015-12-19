package com.puresoltechnologies.ductiledb.core.tx;

import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.PROPERTIES_COLUMN_FAMILY_BYTES;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.DuctileDBVertexImpl;
import com.puresoltechnologies.ductiledb.core.schema.SchemaTable;
import com.puresoltechnologies.ductiledb.core.utils.ElementUtils;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

public class SetVertexPropertyOperation extends AbstractTxOperation {

    private final DuctileDBVertex vertex;
    private final String key;
    private final Object value;

    public SetVertexPropertyOperation(DuctileDBTransactionImpl transaction, DuctileDBVertex vertex, String key,
	    Object value) {
	super(transaction);
	this.vertex = vertex;
	this.key = key;
	this.value = value;
    }

    @Override
    public void commitInternally() {
	DuctileDBTransactionImpl transaction = getTransaction();
	Map<String, Object> properties = ElementUtils.getProperties(vertex);
	properties.put(key, value);
	DuctileDBVertexImpl cachedVertex = new DuctileDBVertexImpl(transaction.getGraph(), vertex.getId(),
		ElementUtils.getLabels(vertex), properties, ElementUtils.getEdges(vertex));
	transaction.setCachedVertex(cachedVertex);

    }

    @Override
    public void rollbackInternally() {
	// TODO Auto-generated method stub
    }

    @Override
    public void perform() throws IOException {
	long vertexId = vertex.getId();
	byte[] id = IdEncoder.encodeRowId(vertexId);
	Put put = new Put(id);
	put.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, Bytes.toBytes(key),
		SerializationUtils.serialize((Serializable) value));
	Put index = OperationsHelper.createVertexPropertyIndexPut(vertexId, key, value);
	put(SchemaTable.VERTICES.getTableName(), put);
	put(SchemaTable.VERTEX_PROPERTIES.getTableName(), index);
    }
}
