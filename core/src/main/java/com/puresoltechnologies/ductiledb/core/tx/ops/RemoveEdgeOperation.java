package com.puresoltechnologies.ductiledb.core.tx.ops;

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

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.api.EdgeDirection;
import com.puresoltechnologies.ductiledb.core.EdgeKey;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

public class RemoveEdgeOperation extends AbstractTxOperation {

    private final long edgeId;
    private final long inVertexId;
    private final long outVertexId;
    private final String label;
    private final Set<String> edgePropertyKeys;

    public RemoveEdgeOperation(Connection connection, long edgeId, long inVertexId, long outVertexId, String label,
	    Set<String> edgePropertyKeys) {
	super(connection);
	this.edgeId = edgeId;
	this.inVertexId = inVertexId;
	this.outVertexId = outVertexId;
	this.label = label;
	this.edgePropertyKeys = Collections.unmodifiableSet(edgePropertyKeys);
    }

    public long getEdgeId() {
	return edgeId;
    }

    @Override
    public void perform() throws IOException {
	Delete deleteOutEdge = new Delete(IdEncoder.encodeRowId(outVertexId));
	deleteOutEdge.addColumns(EDGES_COLUMN_FAMILY_BYTES,
		new EdgeKey(EdgeDirection.OUT, edgeId, inVertexId, label).encode());
	Delete deleteInEdge = new Delete(IdEncoder.encodeRowId(inVertexId));
	deleteInEdge.addColumns(EDGES_COLUMN_FAMILY_BYTES,
		new EdgeKey(EdgeDirection.IN, edgeId, outVertexId, label).encode());
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

    @Override
    public DuctileDBVertex updateVertex(DuctileDBVertex vertex) {
	return vertex;
    }

    @Override
    public DuctileDBEdge updateEdge(DuctileDBEdge edge) {
	return edge;
    }

}
