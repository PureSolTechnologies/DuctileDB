package com.puresoltechnologies.ductiledb.core.graph.tx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.puresoltechnologies.ductiledb.core.graph.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.core.graph.EdgeDirection;
import com.puresoltechnologies.ductiledb.core.graph.EdgeKey;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseColumnFamily;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseTable;
import com.puresoltechnologies.ductiledb.core.graph.utils.IdEncoder;
import com.puresoltechnologies.ductiledb.engine.Delete;
import com.puresoltechnologies.ductiledb.logstore.Key;

public class RemoveEdgeOperation extends AbstractTxOperation {

    private final DuctileDBEdge edge;
    private final long targetVertexId;
    private final long startVertexId;
    private final String type;
    private final Set<String> edgePropertyKeys;

    public RemoveEdgeOperation(DuctileDBTransactionImpl transaction, DuctileDBEdge edge) {
	super(transaction);
	this.edge = edge;
	this.startVertexId = edge.getStartVertex().getId();
	this.targetVertexId = edge.getTargetVertex().getId();
	this.type = edge.getType();
	this.edgePropertyKeys = Collections.unmodifiableSet(edge.getPropertyKeys());
    }

    @Override
    public void commitInternally() {
	DuctileDBTransactionImpl transaction = getTransaction();
	transaction.removeCachedEdge(edge.getId());
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
    public void rollbackInternally() {
	// Intentionally left empty...
    }

    @Override
    public void perform() throws IOException {
	long edgeId = edge.getId();
	Delete deleteOutEdge = new Delete(Key.of(IdEncoder.encodeRowId(startVertexId)));
	deleteOutEdge.addColumns(DatabaseColumnFamily.EDGES.getKey(),
		Key.of(new EdgeKey(EdgeDirection.OUT, edgeId, targetVertexId, type).encode()));
	Delete deleteInEdge = new Delete(Key.of(IdEncoder.encodeRowId(targetVertexId)));
	deleteInEdge.addColumns(DatabaseColumnFamily.EDGES.getKey(),
		Key.of(new EdgeKey(EdgeDirection.IN, edgeId, startVertexId, type).encode()));
	Delete deleteEdges = new Delete(Key.of(IdEncoder.encodeRowId(edgeId)));
	Delete typeIndex = OperationsHelper.createEdgeTypeIndexDelete(edgeId, type);
	List<Delete> propertyIndices = new ArrayList<>();
	for (String key : edgePropertyKeys) {
	    propertyIndices.add(OperationsHelper.createEdgePropertyIndexDelete(edgeId, key));
	}
	delete(DatabaseTable.VERTICES.getName(), deleteOutEdge);
	delete(DatabaseTable.VERTICES.getName(), deleteInEdge);
	delete(DatabaseTable.EDGES.getName(), deleteEdges);
	delete(DatabaseTable.EDGE_TYPES.getName(), typeIndex);
	delete(DatabaseTable.EDGE_PROPERTIES.getName(), propertyIndices);
    }

}
