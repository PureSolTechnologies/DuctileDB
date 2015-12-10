package com.puresoltechnologies.ductiledb.core.tx.ops;

import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.PROPERTIES_COLUMN_FAMILY_BYTES;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.VERTEX_PROPERTIES_INDEX_TABLE;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.VERTICES_TABLE;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.core.DuctileDBVertexImpl;
import com.puresoltechnologies.ductiledb.core.utils.ElementUtils;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

public class RemoveVertexPropertyOperation extends AbstractTxOperation {

    private final long vertexId;
    private final String key;

    public RemoveVertexPropertyOperation(Connection connection, long vertexId, String key) {
	super(connection);
	this.vertexId = vertexId;
	this.key = key;
    }

    @Override
    public void perform() throws IOException {
	byte[] id = IdEncoder.encodeRowId(vertexId);
	Delete delete = new Delete(id);
	delete.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, Bytes.toBytes(key));
	Delete index = OperationsHelper.createVertexPropertyIndexDelete(vertexId, key);
	delete(VERTICES_TABLE, delete);
	delete(VERTEX_PROPERTIES_INDEX_TABLE, index);
    }

    @Override
    public DuctileDBVertex updateVertex(DuctileDBVertex vertex) {
	if (vertex.getId() != vertexId) {
	    return vertex;
	}
	Map<String, Object> properties = new HashMap<>(ElementUtils.getProperties(vertex));
	properties.remove(key);
	return new DuctileDBVertexImpl((DuctileDBGraphImpl) vertex.getGraph(), vertex.getId(),
		ElementUtils.getLabels(vertex), properties, ElementUtils.getEdges(vertex));
    }

    @Override
    public DuctileDBEdge updateEdge(DuctileDBEdge edge) {
	return edge;
    }
}
