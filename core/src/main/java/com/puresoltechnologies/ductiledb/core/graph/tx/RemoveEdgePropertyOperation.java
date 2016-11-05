package com.puresoltechnologies.ductiledb.core.graph.tx;

import java.io.IOException;
import java.util.NavigableMap;

import com.puresoltechnologies.ductiledb.core.graph.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.core.graph.EdgeDirection;
import com.puresoltechnologies.ductiledb.core.graph.EdgeKey;
import com.puresoltechnologies.ductiledb.core.graph.EdgeValue;
import com.puresoltechnologies.ductiledb.core.graph.manager.DuctileDBGraphManagerException;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseColumnFamily;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseTable;
import com.puresoltechnologies.ductiledb.core.graph.utils.IdEncoder;
import com.puresoltechnologies.ductiledb.core.graph.utils.Serializer;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.Delete;
import com.puresoltechnologies.ductiledb.storage.engine.Get;
import com.puresoltechnologies.ductiledb.storage.engine.Key;
import com.puresoltechnologies.ductiledb.storage.engine.Put;
import com.puresoltechnologies.ductiledb.storage.engine.Result;
import com.puresoltechnologies.ductiledb.storage.engine.TableEngine;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnValue;

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
	try {
	    TableEngine table = getStorageEngine().getTable(getNamespace(), DatabaseTable.VERTICES.getName());
	    byte[] startVertexRowId = IdEncoder.encodeRowId(startVertexId);
	    EdgeKey startVertexEdgeKey = new EdgeKey(EdgeDirection.OUT, edgeId, targetVertexId, type);
	    Result startVertexResult = table.get(new Get(Key.of(startVertexRowId)));
	    if (startVertexResult.isEmpty()) {
		throw new IllegalStateException("Start vertex of edge was not found in graph store.");
	    }
	    NavigableMap<Key, ColumnValue> startVertexEdgeColumnFamily = startVertexResult
		    .getFamilyMap(DatabaseColumnFamily.EDGES.getKey());
	    ColumnValue startVertexPropertyBytes = startVertexEdgeColumnFamily.get(startVertexEdgeKey.encode());
	    EdgeValue startVertexEdgeValue = Serializer.deserialize(startVertexPropertyBytes.getBytes(),
		    EdgeValue.class);
	    startVertexEdgeValue.getProperties().remove(key);
	    Put startVertexPut = new Put(Key.of(startVertexRowId));
	    startVertexPut.addColumn(DatabaseColumnFamily.EDGES.getKey(), Key.of(startVertexEdgeKey.encode()),
		    ColumnValue.of(startVertexEdgeValue.encode()));

	    byte[] targetVertexRowId = IdEncoder.encodeRowId(targetVertexId);
	    EdgeKey targetVertexEdgeKey = new EdgeKey(EdgeDirection.IN, edgeId, startVertexId, type);
	    Result targetVertexResult = table.get(new Get(Key.of(targetVertexRowId)));
	    if (targetVertexResult.isEmpty()) {
		throw new IllegalStateException("Target vertex of edge was not found in graph store.");
	    }
	    NavigableMap<Key, ColumnValue> targetVertexEdgeColumnFamily = targetVertexResult
		    .getFamilyMap(DatabaseColumnFamily.EDGES.getKey());
	    ColumnValue targetVertexPropertyBytes = targetVertexEdgeColumnFamily.get(targetVertexEdgeKey.encode());
	    EdgeValue targetVertexEdgeValue = Serializer.deserialize(targetVertexPropertyBytes.getBytes(),
		    EdgeValue.class);
	    targetVertexEdgeValue.getProperties().remove(key);
	    Put targetVertexPut = new Put(Key.of(targetVertexRowId));
	    targetVertexPut.addColumn(DatabaseColumnFamily.EDGES.getKey(), Key.of(targetVertexEdgeKey.encode()),
		    ColumnValue.of(targetVertexEdgeValue.encode()));

	    Delete edgeDelete = new Delete(Key.of(IdEncoder.encodeRowId(edgeId)));
	    edgeDelete.addColumns(DatabaseColumnFamily.PROPERTIES.getKey(), Key.of(key));

	    Delete index = OperationsHelper.createEdgePropertyIndexDelete(edgeId, key);
	    // Add to transaction
	    put(DatabaseTable.VERTICES.getName(), startVertexPut);
	    put(DatabaseTable.VERTICES.getName(), targetVertexPut);
	    delete(DatabaseTable.EDGES.getName(), edgeDelete);
	    delete(DatabaseTable.EDGE_PROPERTIES.getName(), index);
	} catch (StorageException e) {
	    throw new DuctileDBGraphManagerException("Could not perform the action.", e);
	}
    }
}
