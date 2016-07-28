package com.puresoltechnologies.ductiledb.core.graph.tx;

import java.io.IOException;
import java.util.NavigableMap;

import com.puresoltechnologies.ductiledb.api.graph.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.graph.EdgeDirection;
import com.puresoltechnologies.ductiledb.core.graph.EdgeKey;
import com.puresoltechnologies.ductiledb.core.graph.EdgeValue;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseColumnFamily;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseTable;
import com.puresoltechnologies.ductiledb.core.graph.schema.GraphSchema;
import com.puresoltechnologies.ductiledb.core.graph.utils.IdEncoder;
import com.puresoltechnologies.ductiledb.core.graph.utils.Serializer;
import com.puresoltechnologies.ductiledb.storage.engine.Delete;
import com.puresoltechnologies.ductiledb.storage.engine.Get;
import com.puresoltechnologies.ductiledb.storage.engine.Put;
import com.puresoltechnologies.ductiledb.storage.engine.Result;
import com.puresoltechnologies.ductiledb.storage.engine.Table;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public class RemoveEdgePropertyOperation extends AbstractTxOperation {

    private final long edgeId;
    private final long startVertexId;
    private final long targetVertexId;
    private final String type;
    private final String key;
    private final Object oldValue;

    public RemoveEdgePropertyOperation(DuctileDBTransactionImpl transaction, DuctileDBEdge edge, String key) {
	super(transaction);
	this.edgeId = edge.getId();
	this.startVertexId = edge.getStartVertex().getId();
	this.targetVertexId = edge.getTargetVertex().getId();
	this.type = edge.getType();
	this.key = key;
	this.oldValue = edge.getProperty(key);
    }

    @Override
    public void commitInternally() {
	DuctileDBCacheEdge edge = getTransaction().getCachedEdge(edgeId);
	edge.removeProperty(key);
    }

    @Override
    public void rollbackInternally() {
	if (oldValue != null) {
	    DuctileDBCacheEdge edge = getTransaction().getCachedEdge(edgeId);
	    edge.setProperty(key, oldValue);
	}
    }

    @Override
    public void perform() throws IOException {
	try (Table table = getStorageEngine().getTable(GraphSchema.DUCTILEDB_NAMESPACE,
		DatabaseTable.VERTICES.getName())) {
	    byte[] startVertexRowId = IdEncoder.encodeRowId(startVertexId);
	    EdgeKey startVertexEdgeKey = new EdgeKey(EdgeDirection.OUT, edgeId, targetVertexId, type);
	    Result startVertexResult = table.get(new Get(startVertexRowId));
	    if (startVertexResult.isEmpty()) {
		throw new IllegalStateException("Start vertex of edge was not found in graph store.");
	    }
	    NavigableMap<byte[], byte[]> startVertexEdgeColumnFamily = startVertexResult
		    .getFamilyMap(DatabaseColumnFamily.EDGES.getNameBytes());
	    byte[] startVertexPropertyBytes = startVertexEdgeColumnFamily.get(startVertexEdgeKey.encode());
	    EdgeValue startVertexEdgeValue = Serializer.deserialize(startVertexPropertyBytes, EdgeValue.class);
	    startVertexEdgeValue.getProperties().remove(key);
	    Put startVertexPut = new Put(startVertexRowId);
	    startVertexPut.addColumn(DatabaseColumnFamily.EDGES.getNameBytes(), startVertexEdgeKey.encode(),
		    startVertexEdgeValue.encode());

	    byte[] targetVertexRowId = IdEncoder.encodeRowId(targetVertexId);
	    EdgeKey targetVertexEdgeKey = new EdgeKey(EdgeDirection.IN, edgeId, startVertexId, type);
	    Result targetVertexResult = table.get(new Get(targetVertexRowId));
	    if (targetVertexResult.isEmpty()) {
		throw new IllegalStateException("Target vertex of edge was not found in graph store.");
	    }
	    NavigableMap<byte[], byte[]> targetVertexEdgeColumnFamily = targetVertexResult
		    .getFamilyMap(DatabaseColumnFamily.EDGES.getNameBytes());
	    byte[] targetVertexPropertyBytes = targetVertexEdgeColumnFamily.get(targetVertexEdgeKey.encode());
	    EdgeValue targetVertexEdgeValue = Serializer.deserialize(targetVertexPropertyBytes, EdgeValue.class);
	    targetVertexEdgeValue.getProperties().remove(key);
	    Put targetVertexPut = new Put(targetVertexRowId);
	    targetVertexPut.addColumn(DatabaseColumnFamily.EDGES.getNameBytes(), targetVertexEdgeKey.encode(),
		    targetVertexEdgeValue.encode());

	    Delete edgeDelete = new Delete(IdEncoder.encodeRowId(edgeId));
	    edgeDelete.addColumns(DatabaseColumnFamily.PROPERTIES.getNameBytes(), Bytes.toBytes(key));

	    Delete index = OperationsHelper.createEdgePropertyIndexDelete(edgeId, key);
	    // Add to transaction
	    put(DatabaseTable.VERTICES.getName(), startVertexPut);
	    put(DatabaseTable.VERTICES.getName(), targetVertexPut);
	    delete(DatabaseTable.EDGES.getName(), edgeDelete);
	    delete(DatabaseTable.EDGE_PROPERTIES.getName(), index);
	}
    }
}
