package com.puresoltechnologies.ductiledb.core.tx.ops;

import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.EDGES_COLUMN_FAMILY_BYTES;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.EDGES_TABLE;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.EDGE_LABELS_INDEX_TABLE;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.EDGE_PROPERTIES_INDEX_TABLE;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.LABELS_COLUMN_FAMILIY_BYTES;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.PROPERTIES_COLUMN_FAMILY_BYTES;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.START_VERTEXID_COLUMN_BYTES;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.TARGET_VERTEXID_COLUMN_BYTES;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.VERICES_COLUMN_FAMILY_BYTES;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.VERTICES_TABLE;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.api.EdgeDirection;
import com.puresoltechnologies.ductiledb.core.EdgeKey;
import com.puresoltechnologies.ductiledb.core.EdgeValue;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

public class AddEdgeOperation extends AbstractTxOperation {

    private final long edgeId;
    private final long startVertexId;
    private final long targetVertexId;
    private final String label;
    private final Map<String, Object> properties;

    public AddEdgeOperation(Connection connection, long edgeId, long startVertexId, long targetVertexId, String label,
	    Map<String, Object> properties) {
	super(connection);
	this.edgeId = edgeId;
	this.startVertexId = startVertexId;
	this.targetVertexId = targetVertexId;
	this.label = label;
	this.properties = Collections.unmodifiableMap(properties);
    }

    public long getEdgeId() {
	return edgeId;
    }

    public long getStartVertexId() {
	return startVertexId;
    }

    public long getTargetVertexId() {
	return targetVertexId;
    }

    public String getLabel() {
	return label;
    }

    public Map<String, Object> getProperties() {
	return properties;
    }

    @Override
    public void perform() throws IOException {
	byte[] edgeValue = new EdgeValue(properties).encode();
	// Put to Start Vertex
	Put outPut = new Put(IdEncoder.encodeRowId(startVertexId));
	outPut.addColumn(EDGES_COLUMN_FAMILY_BYTES,
		new EdgeKey(EdgeDirection.OUT, edgeId, targetVertexId, label).encode(), edgeValue);
	// Put to Target Vertex
	Put inPut = new Put(IdEncoder.encodeRowId(targetVertexId));
	inPut.addColumn(EDGES_COLUMN_FAMILY_BYTES, new EdgeKey(EdgeDirection.IN, edgeId, startVertexId, label).encode(),
		edgeValue);
	// Put to Edges Table
	Put edgePut = new Put(IdEncoder.encodeRowId(edgeId));
	edgePut.addColumn(VERICES_COLUMN_FAMILY_BYTES, START_VERTEXID_COLUMN_BYTES,
		IdEncoder.encodeRowId(startVertexId));
	edgePut.addColumn(VERICES_COLUMN_FAMILY_BYTES, TARGET_VERTEXID_COLUMN_BYTES,
		IdEncoder.encodeRowId(targetVertexId));
	edgePut.addColumn(LABELS_COLUMN_FAMILIY_BYTES, Bytes.toBytes(label), new byte[0]);
	Put labelIndexPut = OperationsHelper.createEdgeLabelIndexPut(edgeId, label);
	List<Put> propertyIndexPuts = new ArrayList<>();
	for (Entry<String, Object> property : properties.entrySet()) {
	    edgePut.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, Bytes.toBytes(property.getKey()),
		    SerializationUtils.serialize((Serializable) property.getValue()));
	    propertyIndexPuts
		    .add(OperationsHelper.createEdgePropertyIndexPut(edgeId, property.getKey(), property.getValue()));
	}
	put(VERTICES_TABLE, outPut);
	put(VERTICES_TABLE, inPut);
	put(EDGES_TABLE, edgePut);
	put(EDGE_LABELS_INDEX_TABLE, labelIndexPut);
	put(EDGE_PROPERTIES_INDEX_TABLE, propertyIndexPuts);
    }

    @Override
    public DuctileDBVertex updateVertex(DuctileDBVertex vertex) {
	return vertex;
    }

    @Override
    public DuctileDBEdge updateEdge(DuctileDBEdge edge) {
	return edge;
    }

}
