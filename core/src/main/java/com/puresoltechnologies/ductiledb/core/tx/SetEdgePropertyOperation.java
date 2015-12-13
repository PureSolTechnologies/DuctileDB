package com.puresoltechnologies.ductiledb.core.tx;

import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.EDGES_COLUMN_FAMILY_BYTES;
import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.PROPERTIES_COLUMN_FAMILY_BYTES;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.EdgeDirection;
import com.puresoltechnologies.ductiledb.core.DuctileDBEdgeImpl;
import com.puresoltechnologies.ductiledb.core.EdgeKey;
import com.puresoltechnologies.ductiledb.core.EdgeValue;
import com.puresoltechnologies.ductiledb.core.schema.SchemaTable;
import com.puresoltechnologies.ductiledb.core.utils.ElementUtils;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

public class SetEdgePropertyOperation extends AbstractTxOperation {

    private final long edgeId;
    private final long startVertexId;
    private final long targetVertexId;
    private final String label;
    private final String key;
    private final Object value;

    public SetEdgePropertyOperation(DuctileDBTransactionImpl transaction, DuctileDBEdge edge, String key,
	    Object value) {
	super(transaction);
	this.edgeId = edge.getId();
	this.startVertexId = edge.getStartVertex().getId();
	this.targetVertexId = edge.getTargetVertex().getId();
	this.label = edge.getLabel();
	this.key = key;
	this.value = value;
	Map<String, Object> properties = ElementUtils.getProperties(edge);
	properties.put(key, value);
	DuctileDBEdgeImpl cachedVertex = new DuctileDBEdgeImpl(transaction.getGraph(), edge.getId(), edge.getLabel(),
		edge.getStartVertex().getId(), edge.getTargetVertex().getId(), properties);
	transaction.setCachedEdge(cachedVertex);
    }

    @Override
    public void perform() throws IOException {
	try (Table table = getConnection().getTable(SchemaTable.VERTICES.getTableName())) {
	    byte[] startVertexRowId = IdEncoder.encodeRowId(startVertexId);
	    EdgeKey startVertexEdgeKey = new EdgeKey(EdgeDirection.OUT, edgeId, targetVertexId, label);
	    Result startVertexResult = table.get(new Get(startVertexRowId));
	    if (startVertexResult.isEmpty()) {
		throw new IllegalStateException("Start vertex of edge was not found in graph store.");
	    }
	    NavigableMap<byte[], byte[]> startVertexEdgeColumnFamily = startVertexResult
		    .getFamilyMap(EDGES_COLUMN_FAMILY_BYTES);
	    byte[] startVertexPropertyBytes = startVertexEdgeColumnFamily.get(startVertexEdgeKey.encode());
	    EdgeValue startVertexEdgeValue = (EdgeValue) SerializationUtils.deserialize(startVertexPropertyBytes);
	    startVertexEdgeValue.getProperties().put(key, value);
	    Put startVertexPut = new Put(startVertexRowId);
	    startVertexPut.addColumn(EDGES_COLUMN_FAMILY_BYTES, startVertexEdgeKey.encode(),
		    startVertexEdgeValue.encode());

	    byte[] targetVertexRowId = IdEncoder.encodeRowId(targetVertexId);
	    EdgeKey targetVertexEdgeKey = new EdgeKey(EdgeDirection.IN, edgeId, startVertexId, label);
	    Result targetVertexResult = table.get(new Get(targetVertexRowId));
	    if (targetVertexResult.isEmpty()) {
		throw new IllegalStateException("Target vertex of edge was not found in graph store.");
	    }
	    NavigableMap<byte[], byte[]> targetVertexEdgeColumnFamily = targetVertexResult
		    .getFamilyMap(EDGES_COLUMN_FAMILY_BYTES);
	    byte[] targetVertexPropertyBytes = targetVertexEdgeColumnFamily.get(targetVertexEdgeKey.encode());
	    EdgeValue targetVertexEdgeValue = (EdgeValue) SerializationUtils.deserialize(targetVertexPropertyBytes);
	    targetVertexEdgeValue.getProperties().put(key, value);
	    Put targetVertexPut = new Put(targetVertexRowId);
	    targetVertexPut.addColumn(EDGES_COLUMN_FAMILY_BYTES, targetVertexEdgeKey.encode(),
		    targetVertexEdgeValue.encode());

	    Put edgePut = new Put(IdEncoder.encodeRowId(edgeId));
	    edgePut.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, Bytes.toBytes(key),
		    SerializationUtils.serialize((Serializable) value));

	    Put index = OperationsHelper.createEdgePropertyIndexPut(edgeId, key, value);
	    // Add to transaction
	    put(SchemaTable.VERTICES.getTableName(), startVertexPut);
	    put(SchemaTable.VERTICES.getTableName(), targetVertexPut);
	    put(SchemaTable.EDGES.getTableName(), edgePut);
	    put(SchemaTable.EDGE_PROPERTIES.getTableName(), index);
	}
    }
}
