package com.puresoltechnologies.ductiledb.core.graph.tx;

import java.io.IOException;
import java.io.Serializable;
import java.util.NavigableMap;

import com.puresoltechnologies.ductiledb.api.graph.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.graph.EdgeDirection;
import com.puresoltechnologies.ductiledb.core.graph.EdgeKey;
import com.puresoltechnologies.ductiledb.core.graph.EdgeValue;
import com.puresoltechnologies.ductiledb.core.graph.schema.HBaseColumnFamily;
import com.puresoltechnologies.ductiledb.core.graph.schema.HBaseTable;
import com.puresoltechnologies.ductiledb.core.graph.utils.IdEncoder;
import com.puresoltechnologies.ductiledb.core.graph.utils.Serializer;
import com.puresoltechnologies.ductiledb.storage.engine.Get;
import com.puresoltechnologies.ductiledb.storage.engine.Put;
import com.puresoltechnologies.ductiledb.storage.engine.Result;
import com.puresoltechnologies.ductiledb.storage.engine.Table;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;

public class SetEdgePropertyOperation extends AbstractTxOperation {

    private final long edgeId;
    private final long startVertexId;
    private final long targetVertexId;
    private final String type;
    private final String key;
    private final Object value;
    private final Object oldValue;

    public SetEdgePropertyOperation(DuctileDBTransactionImpl transaction, DuctileDBEdge edge, String key,
	    Object value) {
	super(transaction);
	this.edgeId = edge.getId();
	this.startVertexId = edge.getStartVertex().getId();
	this.targetVertexId = edge.getTargetVertex().getId();
	this.type = edge.getType();
	this.key = key;
	this.value = value;
	this.oldValue = edge.getProperty(key);
    }

    @Override
    public void commitInternally() {
	DuctileDBCacheEdge edge = getTransaction().getCachedEdge(edgeId);
	edge.setProperty(key, value);
    }

    @Override
    public void rollbackInternally() {
	DuctileDBCacheEdge edge = getTransaction().getCachedEdge(edgeId);
	if (oldValue == null) {
	    edge.removeProperty(key);
	} else {
	    edge.setProperty(key, oldValue);
	}
    }

    @Override
    public void perform() throws IOException {
	try (Table table = getStorageEngine().getTable(HBaseTable.VERTICES.getName())) {
	    byte[] startVertexRowId = IdEncoder.encodeRowId(startVertexId);
	    EdgeKey startVertexEdgeKey = new EdgeKey(EdgeDirection.OUT, edgeId, targetVertexId, type);
	    Result startVertexResult = table.get(new Get(startVertexRowId));
	    if (startVertexResult.isEmpty()) {
		throw new IllegalStateException("Start vertex of edge was not found in graph store.");
	    }
	    NavigableMap<byte[], byte[]> startVertexEdgeColumnFamily = startVertexResult
		    .getFamilyMap(HBaseColumnFamily.EDGES.getName());
	    byte[] startVertexPropertyBytes = startVertexEdgeColumnFamily.get(startVertexEdgeKey.encode());
	    EdgeValue startVertexEdgeValue = EdgeValue.decode(startVertexPropertyBytes);
	    startVertexEdgeValue.getProperties().put(key, value);
	    Put startVertexPut = new Put(startVertexRowId);
	    startVertexPut.addColumn(HBaseColumnFamily.EDGES.getName(), startVertexEdgeKey.encode(),
		    startVertexEdgeValue.encode());

	    byte[] targetVertexRowId = IdEncoder.encodeRowId(targetVertexId);
	    EdgeKey targetVertexEdgeKey = new EdgeKey(EdgeDirection.IN, edgeId, startVertexId, type);
	    Result targetVertexResult = table.get(new Get(targetVertexRowId));
	    if (targetVertexResult.isEmpty()) {
		throw new IllegalStateException("Target vertex of edge was not found in graph store.");
	    }
	    NavigableMap<byte[], byte[]> targetVertexEdgeColumnFamily = targetVertexResult
		    .getFamilyMap(HBaseColumnFamily.EDGES.getName());
	    byte[] targetVertexPropertyBytes = targetVertexEdgeColumnFamily.get(targetVertexEdgeKey.encode());
	    EdgeValue targetVertexEdgeValue = EdgeValue.decode(targetVertexPropertyBytes);
	    targetVertexEdgeValue.getProperties().put(key, value);
	    Put targetVertexPut = new Put(targetVertexRowId);
	    targetVertexPut.addColumn(HBaseColumnFamily.EDGES.getName(), targetVertexEdgeKey.encode(),
		    targetVertexEdgeValue.encode());

	    Put edgePut = new Put(IdEncoder.encodeRowId(edgeId));
	    edgePut.addColumn(HBaseColumnFamily.PROPERTIES.getName(), Bytes.toBytes(key),
		    Serializer.serializePropertyValue((Serializable) value));

	    Put index = OperationsHelper.createEdgePropertyIndexPut(edgeId, key, (Serializable) value);
	    // Add to transaction
	    put(HBaseTable.VERTICES.getName(), startVertexPut);
	    put(HBaseTable.VERTICES.getName(), targetVertexPut);
	    put(HBaseTable.EDGES.getName(), edgePut);
	    put(HBaseTable.EDGE_PROPERTIES.getName(), index);
	}
    }
}
