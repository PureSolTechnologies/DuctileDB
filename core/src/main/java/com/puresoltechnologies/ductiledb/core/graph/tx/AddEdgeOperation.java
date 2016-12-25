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

import com.puresoltechnologies.ductiledb.core.graph.EdgeDirection;
import com.puresoltechnologies.ductiledb.core.graph.EdgeKey;
import com.puresoltechnologies.ductiledb.core.graph.EdgeValue;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseColumn;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseColumnFamily;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseTable;
import com.puresoltechnologies.ductiledb.core.graph.utils.IdEncoder;
import com.puresoltechnologies.ductiledb.core.graph.utils.Serializer;
import com.puresoltechnologies.ductiledb.engine.Put;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnValue;
import com.puresoltechnologies.ductiledb.logstore.Key;

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
	ColumnValue edgeValue = ColumnValue.of(new EdgeValue(properties).encode());
	// Put to Start Vertex
	Put outPut = new Put(Key.of(IdEncoder.encodeRowId(startVertexId)));
	outPut.addColumn(DatabaseColumnFamily.EDGES.getKey(),
		Key.of(new EdgeKey(EdgeDirection.OUT, edgeId, targetVertexId, type).encode()), edgeValue);
	// Put to Target Vertex
	Put inPut = new Put(Key.of(IdEncoder.encodeRowId(targetVertexId)));
	inPut.addColumn(DatabaseColumnFamily.EDGES.getKey(),
		Key.of(new EdgeKey(EdgeDirection.IN, edgeId, startVertexId, type).encode()), edgeValue);
	// Put to Edges Table
	Put edgePut = new Put(Key.of(IdEncoder.encodeRowId(edgeId)));
	edgePut.addColumn(DatabaseColumnFamily.VERICES.getKey(), DatabaseColumn.START_VERTEX_ID.getKey(),
		ColumnValue.of(IdEncoder.encodeRowId(startVertexId)));
	edgePut.addColumn(DatabaseColumnFamily.VERICES.getKey(), DatabaseColumn.TARGET_VERTEX_ID.getKey(),
		ColumnValue.of(IdEncoder.encodeRowId(targetVertexId)));
	edgePut.addColumn(DatabaseColumnFamily.TYPES.getKey(), Key.of(type), ColumnValue.empty());
	Put typeIndexPut = OperationsHelper.createEdgeTypeIndexPut(edgeId, type);
	List<Put> propertyIndexPuts = new ArrayList<>();
	edgePut.addColumn(DatabaseColumnFamily.PROPERTIES.getKey(), Key.of(DUCTILEDB_ID_PROPERTY),
		ColumnValue.of(Serializer.serializePropertyValue(edgeId)));
	edgePut.addColumn(DatabaseColumnFamily.PROPERTIES.getKey(), Key.of(DUCTILEDB_CREATE_TIMESTAMP_PROPERTY),
		ColumnValue.of(Serializer.serializePropertyValue(new Date())));
	for (Entry<String, Object> property : properties.entrySet()) {
	    edgePut.addColumn(DatabaseColumnFamily.PROPERTIES.getKey(), Key.of(property.getKey()),
		    ColumnValue.of(Serializer.serializePropertyValue((Serializable) property.getValue())));
	    propertyIndexPuts.add(OperationsHelper.createEdgePropertyIndexPut(edgeId, property.getKey(),
		    (Serializable) property.getValue()));
	}
	put(DatabaseTable.VERTICES.getName(), outPut);
	put(DatabaseTable.VERTICES.getName(), inPut);
	put(DatabaseTable.EDGES.getName(), edgePut);
	put(DatabaseTable.EDGE_TYPES.getName(), typeIndexPut);
	put(DatabaseTable.EDGE_PROPERTIES.getName(), propertyIndexPuts);
    }
}
