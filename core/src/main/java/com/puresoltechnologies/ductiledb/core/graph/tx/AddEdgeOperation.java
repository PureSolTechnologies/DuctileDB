package com.puresoltechnologies.ductiledb.core.graph.tx;

import static com.puresoltechnologies.ductiledb.core.graph.schema.GraphSchema.DUCTILEDB_CREATE_TIMESTAMP_PROPERTY;
import static com.puresoltechnologies.ductiledb.core.graph.schema.GraphSchema.DUCTILEDB_ID_PROPERTY;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.puresoltechnologies.ductiledb.api.graph.EdgeDirection;
import com.puresoltechnologies.ductiledb.core.graph.EdgeKey;
import com.puresoltechnologies.ductiledb.core.graph.EdgeValue;
import com.puresoltechnologies.ductiledb.core.graph.schema.HBaseColumn;
import com.puresoltechnologies.ductiledb.core.graph.schema.HBaseColumnFamily;
import com.puresoltechnologies.ductiledb.core.graph.schema.HBaseTable;
import com.puresoltechnologies.ductiledb.core.graph.utils.IdEncoder;
import com.puresoltechnologies.ductiledb.core.graph.utils.Serializer;
import com.puresoltechnologies.ductiledb.storage.engine.Put;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

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
	this.edge = new DuctileDBCacheEdge(transaction, edgeId, type, startVertexId, targetVertexId, properties);
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
	outPut.addColumn(HBaseColumnFamily.EDGES.getNameBytes(),
		new EdgeKey(EdgeDirection.OUT, edgeId, targetVertexId, type).encode(), edgeValue);
	// Put to Target Vertex
	Put inPut = new Put(IdEncoder.encodeRowId(targetVertexId));
	inPut.addColumn(HBaseColumnFamily.EDGES.getNameBytes(),
		new EdgeKey(EdgeDirection.IN, edgeId, startVertexId, type).encode(), edgeValue);
	// Put to Edges Table
	Put edgePut = new Put(IdEncoder.encodeRowId(edgeId));
	edgePut.addColumn(HBaseColumnFamily.VERICES.getNameBytes(), HBaseColumn.START_VERTEX_ID.getNameBytes(),
		IdEncoder.encodeRowId(startVertexId));
	edgePut.addColumn(HBaseColumnFamily.VERICES.getNameBytes(), HBaseColumn.TARGET_VERTEX_ID.getNameBytes(),
		IdEncoder.encodeRowId(targetVertexId));
	edgePut.addColumn(HBaseColumnFamily.TYPES.getNameBytes(), Bytes.toBytes(type), new byte[0]);
	Put typeIndexPut = OperationsHelper.createEdgeTypeIndexPut(edgeId, type);
	List<Put> propertyIndexPuts = new ArrayList<>();
	edgePut.addColumn(HBaseColumnFamily.PROPERTIES.getNameBytes(), Bytes.toBytes(DUCTILEDB_ID_PROPERTY),
		Serializer.serializePropertyValue(edgeId));
	edgePut.addColumn(HBaseColumnFamily.PROPERTIES.getNameBytes(),
		Bytes.toBytes(DUCTILEDB_CREATE_TIMESTAMP_PROPERTY), Serializer.serializePropertyValue(new Date()));
	for (Entry<String, Object> property : properties.entrySet()) {
	    edgePut.addColumn(HBaseColumnFamily.PROPERTIES.getNameBytes(), Bytes.toBytes(property.getKey()),
		    Serializer.serializePropertyValue((Serializable) property.getValue()));
	    propertyIndexPuts.add(OperationsHelper.createEdgePropertyIndexPut(edgeId, property.getKey(),
		    (Serializable) property.getValue()));
	}
	put(HBaseTable.VERTICES.getName(), outPut);
	put(HBaseTable.VERTICES.getName(), inPut);
	put(HBaseTable.EDGES.getName(), edgePut);
	put(HBaseTable.EDGE_TYPES.getName(), typeIndexPut);
	put(HBaseTable.EDGE_PROPERTIES.getName(), propertyIndexPuts);
    }
}
