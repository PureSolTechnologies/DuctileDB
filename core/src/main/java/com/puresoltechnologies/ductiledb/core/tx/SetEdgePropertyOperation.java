package com.puresoltechnologies.ductiledb.core.tx;

import java.io.IOException;
import java.io.Serializable;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.EdgeDirection;
import com.puresoltechnologies.ductiledb.core.EdgeKey;
import com.puresoltechnologies.ductiledb.core.EdgeValue;
import com.puresoltechnologies.ductiledb.core.schema.HBaseColumnFamily;
import com.puresoltechnologies.ductiledb.core.schema.HBaseTable;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;
import com.puresoltechnologies.ductiledb.core.utils.Serializer;

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
	try (Table table = getConnection().getTable(HBaseTable.VERTICES.getTableName())) {
	    byte[] startVertexRowId = IdEncoder.encodeRowId(startVertexId);
	    EdgeKey startVertexEdgeKey = new EdgeKey(EdgeDirection.OUT, edgeId, targetVertexId, type);
	    Result startVertexResult = table.get(new Get(startVertexRowId));
	    if (startVertexResult.isEmpty()) {
		throw new IllegalStateException("Start vertex of edge was not found in graph store.");
	    }
	    NavigableMap<byte[], byte[]> startVertexEdgeColumnFamily = startVertexResult
		    .getFamilyMap(HBaseColumnFamily.EDGES.getNameBytes());
	    byte[] startVertexPropertyBytes = startVertexEdgeColumnFamily.get(startVertexEdgeKey.encode());
	    EdgeValue startVertexEdgeValue = EdgeValue.decode(startVertexPropertyBytes);
	    startVertexEdgeValue.getProperties().put(key, value);
	    Put startVertexPut = new Put(startVertexRowId);
	    startVertexPut.addColumn(HBaseColumnFamily.EDGES.getNameBytes(), startVertexEdgeKey.encode(),
		    startVertexEdgeValue.encode());

	    byte[] targetVertexRowId = IdEncoder.encodeRowId(targetVertexId);
	    EdgeKey targetVertexEdgeKey = new EdgeKey(EdgeDirection.IN, edgeId, startVertexId, type);
	    Result targetVertexResult = table.get(new Get(targetVertexRowId));
	    if (targetVertexResult.isEmpty()) {
		throw new IllegalStateException("Target vertex of edge was not found in graph store.");
	    }
	    NavigableMap<byte[], byte[]> targetVertexEdgeColumnFamily = targetVertexResult
		    .getFamilyMap(HBaseColumnFamily.EDGES.getNameBytes());
	    byte[] targetVertexPropertyBytes = targetVertexEdgeColumnFamily.get(targetVertexEdgeKey.encode());
	    EdgeValue targetVertexEdgeValue = EdgeValue.decode(targetVertexPropertyBytes);
	    targetVertexEdgeValue.getProperties().put(key, value);
	    Put targetVertexPut = new Put(targetVertexRowId);
	    targetVertexPut.addColumn(HBaseColumnFamily.EDGES.getNameBytes(), targetVertexEdgeKey.encode(),
		    targetVertexEdgeValue.encode());

	    Put edgePut = new Put(IdEncoder.encodeRowId(edgeId));
	    edgePut.addColumn(HBaseColumnFamily.PROPERTIES.getNameBytes(), Bytes.toBytes(key),
		    Serializer.serializePropertyValue((Serializable) value));

	    Put index = OperationsHelper.createEdgePropertyIndexPut(edgeId, key, (Serializable) value);
	    // Add to transaction
	    put(HBaseTable.VERTICES.getTableName(), startVertexPut);
	    put(HBaseTable.VERTICES.getTableName(), targetVertexPut);
	    put(HBaseTable.EDGES.getTableName(), edgePut);
	    put(HBaseTable.EDGE_PROPERTIES.getTableName(), index);
	}
    }
}
