package com.puresoltechnologies.ductiledb.core.tx;

import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.DUCTILEDB_CREATE_TIMESTAMP_PROPERTY;
import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.DUCTILEDB_ID_PROPERTY;
import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.EDGES_COLUMN_FAMILY_BYTES;
import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.LABELS_COLUMN_FAMILIY_BYTES;
import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.PROPERTIES_COLUMN_FAMILY_BYTES;
import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.START_VERTEXID_COLUMN_BYTES;
import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.TARGET_VERTEXID_COLUMN_BYTES;
import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.VERICES_COLUMN_FAMILY_BYTES;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.api.EdgeDirection;
import com.puresoltechnologies.ductiledb.core.DuctileDBEdgeImpl;
import com.puresoltechnologies.ductiledb.core.DuctileDBVertexImpl;
import com.puresoltechnologies.ductiledb.core.EdgeKey;
import com.puresoltechnologies.ductiledb.core.EdgeValue;
import com.puresoltechnologies.ductiledb.core.schema.SchemaTable;
import com.puresoltechnologies.ductiledb.core.utils.ElementUtils;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

public class AddEdgeOperation extends AbstractTxOperation {

    private final long edgeId;
    private final long startVertexId;
    private final long targetVertexId;
    private final String label;
    private final Map<String, Object> properties;

    public AddEdgeOperation(DuctileDBTransactionImpl transaction, long edgeId, long startVertexId, long targetVertexId,
	    String label, Map<String, Object> properties) {
	super(transaction);
	this.edgeId = edgeId;
	this.startVertexId = startVertexId;
	this.targetVertexId = targetVertexId;
	this.label = label;
	this.properties = Collections.unmodifiableMap(properties);
	DuctileDBEdgeImpl edge = new DuctileDBEdgeImpl(transaction.getGraph(), edgeId, label, startVertexId,
		targetVertexId, properties);
	transaction.setCachedEdge(edge);
	{
	    DuctileDBVertex cachedStartVertex = transaction.getVertex(startVertexId);
	    if (cachedStartVertex != null) {
		List<DuctileDBEdge> edges = ElementUtils.getEdges(cachedStartVertex);
		edges.add(edge);
		transaction.setCachedVertex(new DuctileDBVertexImpl(transaction.getGraph(), cachedStartVertex.getId(),
			ElementUtils.getLabels(cachedStartVertex), ElementUtils.getProperties(cachedStartVertex),
			edges));
	    }
	}
	{
	    DuctileDBVertex cachedTargetVertex = transaction.getVertex(targetVertexId);
	    if (cachedTargetVertex != null) {
		List<DuctileDBEdge> edges = ElementUtils.getEdges(cachedTargetVertex);
		edges.add(edge);
		transaction.setCachedVertex(new DuctileDBVertexImpl(transaction.getGraph(), cachedTargetVertex.getId(),
			ElementUtils.getLabels(cachedTargetVertex), ElementUtils.getProperties(cachedTargetVertex),
			edges));
	    }
	}
    }

    public long getEdgeId() {
	return edgeId;
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
	edgePut.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, Bytes.toBytes(DUCTILEDB_ID_PROPERTY),
		SerializationUtils.serialize(edgeId));
	edgePut.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, Bytes.toBytes(DUCTILEDB_CREATE_TIMESTAMP_PROPERTY),
		SerializationUtils.serialize(new Date()));
	for (Entry<String, Object> property : properties.entrySet()) {
	    edgePut.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, Bytes.toBytes(property.getKey()),
		    SerializationUtils.serialize((Serializable) property.getValue()));
	    propertyIndexPuts
		    .add(OperationsHelper.createEdgePropertyIndexPut(edgeId, property.getKey(), property.getValue()));
	}
	put(SchemaTable.VERTICES.getTableName(), outPut);
	put(SchemaTable.VERTICES.getTableName(), inPut);
	put(SchemaTable.EDGES.getTableName(), edgePut);
	put(SchemaTable.EDGE_LABELS.getTableName(), labelIndexPut);
	put(SchemaTable.EDGE_PROPERTIES.getTableName(), propertyIndexPuts);
    }
}
