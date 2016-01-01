package com.puresoltechnologies.ductiledb.core.tx;

import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.DUCTILEDB_CREATE_TIMESTAMP_PROPERTY;
import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.DUCTILEDB_ID_PROPERTY;
import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.EDGES_COLUMN_FAMILY_BYTES;
import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.TYPES_COLUMN_FAMILIY_BYTES;
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

import com.puresoltechnologies.ductiledb.api.EdgeDirection;
import com.puresoltechnologies.ductiledb.core.EdgeKey;
import com.puresoltechnologies.ductiledb.core.EdgeValue;
import com.puresoltechnologies.ductiledb.core.schema.SchemaTable;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

public class AddEdgeOperation extends AbstractTxOperation {

    private final long startVertexId;
    private final long targetVertexId;
    private final String type;
    private final Map<String, Object> properties;
    private final DuctileDBCacheEdge edge;

    public AddEdgeOperation(DuctileDBTransactionImpl transaction, long edgeId, long startVertexId, long targetVertexId,
	    String type, Map<String, Object> properties) {
	super(transaction);
	this.startVertexId = startVertexId;
	this.targetVertexId = targetVertexId;
	this.type = type;
	this.properties = Collections.unmodifiableMap(properties);
	this.edge = new DuctileDBCacheEdge(transaction.getGraph(), edgeId, type, startVertexId, targetVertexId,
		properties);
    }

    public long getEdgeId() {
	return edge.getId();
    }

    @Override
    public void commitInternally() {
	DuctileDBTransactionImpl transaction = getTransaction();
	transaction.setCachedEdge(edge);
	{
	    DuctileDBCacheVertex startVertex = transaction.getCachedVertex(startVertexId);
	    if (startVertex != null) {
		startVertex.addEdge(edge);
	    }
	}
	{
	    DuctileDBCacheVertex targetVertex = transaction.getCachedVertex(targetVertexId);
	    if (targetVertex != null) {
		targetVertex.addEdge(edge);
	    }
	}
    }

    @Override
    public void rollbackInternally() {
	DuctileDBTransactionImpl transaction = getTransaction();
	{
	    DuctileDBCacheVertex startVertex = transaction.getCachedVertex(startVertexId);
	    if (startVertex != null) {
		startVertex.removeEdge(edge);
	    }
	}
	{
	    DuctileDBCacheVertex targetVertex = transaction.getCachedVertex(targetVertexId);
	    if (targetVertex != null) {
		targetVertex.removeEdge(edge);
	    }
	}
    }

    @Override
    public void perform() throws IOException {
	long edgeId = edge.getId();
	byte[] edgeValue = new EdgeValue(properties).encode();
	// Put to Start Vertex
	Put outPut = new Put(IdEncoder.encodeRowId(startVertexId));
	outPut.addColumn(EDGES_COLUMN_FAMILY_BYTES,
		new EdgeKey(EdgeDirection.OUT, edgeId, targetVertexId, type).encode(), edgeValue);
	// Put to Target Vertex
	Put inPut = new Put(IdEncoder.encodeRowId(targetVertexId));
	inPut.addColumn(EDGES_COLUMN_FAMILY_BYTES, new EdgeKey(EdgeDirection.IN, edgeId, startVertexId, type).encode(),
		edgeValue);
	// Put to Edges Table
	Put edgePut = new Put(IdEncoder.encodeRowId(edgeId));
	edgePut.addColumn(VERICES_COLUMN_FAMILY_BYTES, START_VERTEXID_COLUMN_BYTES,
		IdEncoder.encodeRowId(startVertexId));
	edgePut.addColumn(VERICES_COLUMN_FAMILY_BYTES, TARGET_VERTEXID_COLUMN_BYTES,
		IdEncoder.encodeRowId(targetVertexId));
	edgePut.addColumn(TYPES_COLUMN_FAMILIY_BYTES, Bytes.toBytes(type), new byte[0]);
	Put typeIndexPut = OperationsHelper.createEdgeTypeIndexPut(edgeId, type);
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
	put(SchemaTable.EDGE_TYPES.getTableName(), typeIndexPut);
	put(SchemaTable.EDGE_PROPERTIES.getTableName(), propertyIndexPuts);
    }
}
