package com.puresoltechnologies.ductiledb.core.graph.tx;

import java.io.IOException;
import java.io.Serializable;

import com.puresoltechnologies.ductiledb.bigtable.BigTable;
import com.puresoltechnologies.ductiledb.bigtable.Get;
import com.puresoltechnologies.ductiledb.bigtable.Put;
import com.puresoltechnologies.ductiledb.bigtable.Result;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnMap;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnValue;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.core.graph.EdgeDirection;
import com.puresoltechnologies.ductiledb.core.graph.EdgeKey;
import com.puresoltechnologies.ductiledb.core.graph.EdgeValue;
import com.puresoltechnologies.ductiledb.core.graph.manager.DuctileDBGraphManagerException;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseColumnFamily;
import com.puresoltechnologies.ductiledb.core.graph.schema.DatabaseTable;
import com.puresoltechnologies.ductiledb.core.graph.utils.IdEncoder;
import com.puresoltechnologies.ductiledb.core.graph.utils.Serializer;
import com.puresoltechnologies.ductiledb.logstore.Key;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;

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
	try {
	    byte[] startVertexRowId = IdEncoder.encodeRowId(startVertexId);
	    BigTable table = getStorageEngine().getNamespace(getNamespace()).getTable(DatabaseTable.VERTICES.getName());
	    Result startVertexResult = table.get(new Get(Key.of(startVertexRowId)));
	    if (startVertexResult.isEmpty()) {
		throw new IllegalStateException("Start vertex of edge was not found in graph store.");
	    }
	    ColumnMap startVertexEdgeColumnFamily = startVertexResult.getFamilyMap(DatabaseColumnFamily.EDGES.getKey());
	    EdgeKey startVertexEdgeKey = new EdgeKey(EdgeDirection.OUT, edgeId, targetVertexId, type);
	    ColumnValue startVertexProperty = startVertexEdgeColumnFamily.get(Key.of(startVertexEdgeKey.encode()));
	    EdgeValue startVertexEdgeValue = EdgeValue.decode(startVertexProperty.getBytes());
	    startVertexEdgeValue.getProperties().put(key, value);
	    Put startVertexPut = new Put(Key.of(startVertexRowId));
	    startVertexPut.addColumn(DatabaseColumnFamily.EDGES.getKey(), Key.of(startVertexEdgeKey.encode()),
		    ColumnValue.of(startVertexEdgeValue.encode()));

	    byte[] targetVertexRowId = IdEncoder.encodeRowId(targetVertexId);
	    EdgeKey targetVertexEdgeKey = new EdgeKey(EdgeDirection.IN, edgeId, startVertexId, type);
	    Result targetVertexResult = table.get(new Get(Key.of(targetVertexRowId)));
	    if (targetVertexResult.isEmpty()) {
		throw new IllegalStateException("Target vertex of edge was not found in graph store.");
	    }
	    ColumnMap targetVertexEdgeColumnFamily = targetVertexResult
		    .getFamilyMap(DatabaseColumnFamily.EDGES.getKey());
	    ColumnValue targetVertexProperty = targetVertexEdgeColumnFamily.get(Key.of(targetVertexEdgeKey.encode()));
	    EdgeValue targetVertexEdgeValue = EdgeValue.decode(targetVertexProperty.getBytes());
	    targetVertexEdgeValue.getProperties().put(key, value);
	    Put targetVertexPut = new Put(Key.of(targetVertexRowId));
	    targetVertexPut.addColumn(DatabaseColumnFamily.EDGES.getKey(), Key.of(targetVertexEdgeKey.encode()),
		    ColumnValue.of(targetVertexEdgeValue.encode()));

	    Put edgePut = new Put(Key.of(IdEncoder.encodeRowId(edgeId)));
	    edgePut.addColumn(DatabaseColumnFamily.PROPERTIES.getKey(), Key.of(key),
		    ColumnValue.of(Serializer.serializePropertyValue((Serializable) value)));

	    Put index = OperationsHelper.createEdgePropertyIndexPut(edgeId, key, (Serializable) value);
	    // Add to transaction
	    put(DatabaseTable.VERTICES.getName(), startVertexPut);
	    put(DatabaseTable.VERTICES.getName(), targetVertexPut);
	    put(DatabaseTable.EDGES.getName(), edgePut);
	    put(DatabaseTable.EDGE_PROPERTIES.getName(), index);
	} catch (StorageException e) {
	    throw new DuctileDBGraphManagerException("Could not perform action.", e);
	}
    }
}
