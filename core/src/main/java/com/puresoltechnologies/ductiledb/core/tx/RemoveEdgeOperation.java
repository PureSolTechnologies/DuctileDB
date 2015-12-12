package com.puresoltechnologies.ductiledb.core.tx;

import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.EDGES_COLUMN_FAMILY_BYTES;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.EDGES_TABLE;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.EDGE_LABELS_INDEX_TABLE;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.EDGE_PROPERTIES_INDEX_TABLE;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.VERTICES_TABLE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.client.Delete;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.api.EdgeDirection;
import com.puresoltechnologies.ductiledb.core.DuctileDBEdgeImpl;
import com.puresoltechnologies.ductiledb.core.DuctileDBVertexImpl;
import com.puresoltechnologies.ductiledb.core.EdgeKey;
import com.puresoltechnologies.ductiledb.core.utils.ElementUtils;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

public class RemoveEdgeOperation extends AbstractTxOperation {

    private final long edgeId;
    private final long targetVertexId;
    private final long startVertexId;
    private final String label;
    private final Set<String> edgePropertyKeys;

    public RemoveEdgeOperation(DuctileDBTransactionImpl transaction, DuctileDBEdge edge) {
	super(transaction);
	this.edgeId = edge.getId();
	this.startVertexId = ((DuctileDBEdgeImpl) edge).getStartVertexId();
	this.targetVertexId = ((DuctileDBEdgeImpl) edge).getTargetVertexId();
	this.label = edge.getLabel();
	this.edgePropertyKeys = Collections.unmodifiableSet(edge.getPropertyKeys());
	transaction.removeCachedEdge(edgeId);
	{
	    DuctileDBVertex cachedStartVertex = transaction.getVertex(startVertexId);
	    if (cachedStartVertex != null) {
		List<DuctileDBEdge> edges = ElementUtils.getEdges(cachedStartVertex);
		edges.remove(edge);
		transaction.setCachedVertex(new DuctileDBVertexImpl(transaction.getGraph(), cachedStartVertex.getId(),
			ElementUtils.getLabels(cachedStartVertex), ElementUtils.getProperties(cachedStartVertex),
			edges));
	    }
	}
	{
	    DuctileDBVertex cachedTargetVertex = transaction.getVertex(targetVertexId);
	    if (cachedTargetVertex != null) {
		List<DuctileDBEdge> edges = ElementUtils.getEdges(cachedTargetVertex);
		edges.remove(edge);
		transaction.setCachedVertex(new DuctileDBVertexImpl(transaction.getGraph(), cachedTargetVertex.getId(),
			ElementUtils.getLabels(cachedTargetVertex), ElementUtils.getProperties(cachedTargetVertex),
			edges));
	    } else {

	    }
	}
    }

    public long getEdgeId() {
	return edgeId;
    }

    @Override
    public void perform() throws IOException {
	Delete deleteOutEdge = new Delete(IdEncoder.encodeRowId(startVertexId));
	deleteOutEdge.addColumns(EDGES_COLUMN_FAMILY_BYTES,
		new EdgeKey(EdgeDirection.OUT, edgeId, targetVertexId, label).encode());
	Delete deleteInEdge = new Delete(IdEncoder.encodeRowId(targetVertexId));
	deleteInEdge.addColumns(EDGES_COLUMN_FAMILY_BYTES,
		new EdgeKey(EdgeDirection.IN, edgeId, startVertexId, label).encode());
	Delete deleteEdges = new Delete(IdEncoder.encodeRowId(edgeId));
	Delete labelIndex = OperationsHelper.createEdgeLabelIndexDelete(edgeId, label);
	List<Delete> propertyIndices = new ArrayList<>();
	for (String key : edgePropertyKeys) {
	    propertyIndices.add(OperationsHelper.createEdgePropertyIndexDelete(edgeId, key));
	}
	delete(VERTICES_TABLE, deleteOutEdge);
	delete(VERTICES_TABLE, deleteInEdge);
	delete(EDGES_TABLE, deleteEdges);
	delete(EDGE_LABELS_INDEX_TABLE, labelIndex);
	delete(EDGE_PROPERTIES_INDEX_TABLE, propertyIndices);
    }

}