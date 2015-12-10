package com.puresoltechnologies.ductiledb.core.tx.ops;

import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.PROPERTIES_COLUMN_FAMILY_BYTES;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.VERTEX_PROPERTIES_INDEX_TABLE;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.VERTICES_TABLE;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.core.DuctileDBVertexImpl;
import com.puresoltechnologies.ductiledb.core.utils.ElementUtils;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

public class SetVertexPropertyOperation extends AbstractTxOperation {

    private final long vertexId;
    private final String key;
    private final Object value;

    public SetVertexPropertyOperation(Connection connection, long vertexId, String key, Object value) {
	super(connection);
	this.vertexId = vertexId;
	this.key = key;
	this.value = value;
    }

    @Override
    public void perform() throws IOException {
	byte[] id = IdEncoder.encodeRowId(vertexId);
	Put put = new Put(id);
	put.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, Bytes.toBytes(key),
		SerializationUtils.serialize((Serializable) value));
	Put index = OperationsHelper.createVertexPropertyIndexPut(vertexId, key, value);
	put(VERTICES_TABLE, put);
	put(VERTEX_PROPERTIES_INDEX_TABLE, index);
    }

    @Override
    public DuctileDBVertex updateVertex(DuctileDBVertex vertex) {
	if (vertex.getId() != vertexId) {
	    return vertex;
	}
	Map<String, Object> properties = new HashMap<>(ElementUtils.getProperties(vertex));
	properties.put(key, value);
	return new DuctileDBVertexImpl((DuctileDBGraphImpl) vertex.getGraph(), vertex.getId(),
		ElementUtils.getLabels(vertex), properties, ElementUtils.getEdges(vertex));
    }

    @Override
    public DuctileDBEdge updateEdge(DuctileDBEdge edge) {
	return edge;
    }

}
