package com.puresoltechnologies.ductiledb.core.tx;

import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.EDGES_COLUMN_FAMILY_BYTES;

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
import com.puresoltechnologies.ductiledb.core.schema.SchemaTable;
import com.puresoltechnologies.ductiledb.core.utils.ElementUtils;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

public class RemoveEdgeOperation extends AbstractTxOperation {

    private final DuctileDBEdge edge;
    private final long targetVertexId;
    private final long startVertexId;
    private final String label;
    private final Set<String> edgePropertyKeys;

    public RemoveEdgeOperation(DuctileDBTransactionImpl transaction, DuctileDBEdge edge) {
	super(transaction);
	this.edge = edge;
	this.startVertexId = ((DuctileDBEdgeImpl) edge).getStartVertexId();
	this.targetVertexId = ((DuctileDBEdgeImpl) edge).getTargetVertexId();
	this.label = edge.getLabel();
	this.edgePropertyKeys = Collections.unmodifiableSet(edge.getPropertyKeys());
    }

    @Override
    public void commitInternally() {
	DuctileDBTransactionImpl transaction = getTransaction();
	transaction.removeCachedEdge(edge.getId());
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

    @Override
    public void rollbackInternally() {
	// TODO Auto-generated method stub
    }

    @Override
    public void perform() throws IOException {
	long edgeId = edge.getId();
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
	delete(SchemaTable.VERTICES.getTableName(), deleteOutEdge);
	delete(SchemaTable.VERTICES.getTableName(), deleteInEdge);
	delete(SchemaTable.EDGES.getTableName(), deleteEdges);
	delete(SchemaTable.EDGE_LABELS.getTableName(), labelIndex);
	delete(SchemaTable.EDGE_PROPERTIES.getTableName(), propertyIndices);
    }

}
