package com.puresoltechnologies.ductiledb.core.tx;

import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.DUCTILEDB_ID_PROPERTY;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.EDGEID_COLUMN_BYTES;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.EDGES_TABLE;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.EDGE_LABELS_INDEX_TABLE;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.EDGE_PROPERTIES_INDEX_TABLE;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.ID_ROW_BYTES;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.INDEX_COLUMN_FAMILY_BYTES;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.METADATA_COLUMN_FAMILIY_BYTES;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.METADATA_TABLE;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.VERTEXID_COLUMN_BYTES;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.VERTEX_LABELS_INDEX_TABLE;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.VERTEX_PROPERTIES_INDEX_TABLE;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.VERTICES_TABLE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.api.EdgeDirection;
import com.puresoltechnologies.ductiledb.api.tx.DuctileDBTransaction;
import com.puresoltechnologies.ductiledb.core.DuctileDBEdgeImpl;
import com.puresoltechnologies.ductiledb.core.DuctileDBException;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.core.DuctileDBVertexImpl;
import com.puresoltechnologies.ductiledb.core.EdgeIterable;
import com.puresoltechnologies.ductiledb.core.ResultDecoder;
import com.puresoltechnologies.ductiledb.core.VertexIterable;
import com.puresoltechnologies.ductiledb.core.tx.ops.AddEdgeOperation;
import com.puresoltechnologies.ductiledb.core.tx.ops.AddVertexLabelOperation;
import com.puresoltechnologies.ductiledb.core.tx.ops.AddVertexOperation;
import com.puresoltechnologies.ductiledb.core.tx.ops.RemoveEdgeOperation;
import com.puresoltechnologies.ductiledb.core.tx.ops.RemoveEdgePropertyOperation;
import com.puresoltechnologies.ductiledb.core.tx.ops.RemoveVertexLabelOperation;
import com.puresoltechnologies.ductiledb.core.tx.ops.RemoveVertexOperation;
import com.puresoltechnologies.ductiledb.core.tx.ops.RemoveVertexPropertyOperation;
import com.puresoltechnologies.ductiledb.core.tx.ops.SetEdgePropertyOperation;
import com.puresoltechnologies.ductiledb.core.tx.ops.SetVertexPropertyOperation;
import com.puresoltechnologies.ductiledb.core.tx.ops.TxOperation;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;

/**
 * This transaction is used per thread to record changes in the graph to be
 * commited as batch.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBTransactionImpl implements DuctileDBTransaction {

    static final long ID_CACHE = 100;

    private long vertexIdCounter = ID_CACHE;
    private long edgeIdCounter = ID_CACHE;
    private long nextVertexId = -1;
    private long nextEdgeId = -1;

    public final List<TxOperation> tableOperations = new ArrayList<>();
    private final DuctileDBGraphImpl graph;
    private final Connection connection;
    private boolean closed = false;

    public DuctileDBTransactionImpl(DuctileDBGraphImpl graph) {
	this.graph = graph;
	this.connection = graph.getConnection();

    }

    @Override
    public void commit() {
	checkForClosed();
	try {
	    for (TxOperation operation : tableOperations) {
		operation.perform();
	    }
	    tableOperations.clear();
	} catch (IOException e) {
	    throw new DuctileDBException("Could not cmomit changes.", e);
	}
    }

    @Override
    public void rollback() {
	checkForClosed();
	tableOperations.clear();
    }

    private void checkForClosed() {
	if (closed) {
	    throw new IllegalStateException("Transaction was already closed.");
	}
    }

    @Override
    public void close() {
	tableOperations.clear();
	closed = true;
    }

    Table openMetaDataTable() throws IOException {
	return connection.getTable(TableName.valueOf(METADATA_TABLE));
    }

    Table openVertexTable() throws IOException {
	return connection.getTable(TableName.valueOf(VERTICES_TABLE));
    }

    Table openEdgeTable() throws IOException {
	return connection.getTable(TableName.valueOf(EDGES_TABLE));
    }

    Table openVertexPropertyTable() throws IOException {
	return connection.getTable(TableName.valueOf(VERTEX_PROPERTIES_INDEX_TABLE));
    }

    Table openVertexLabelTable() throws IOException {
	return connection.getTable(TableName.valueOf(VERTEX_LABELS_INDEX_TABLE));
    }

    Table openEdgePropertyTable() throws IOException {
	return connection.getTable(TableName.valueOf(EDGE_PROPERTIES_INDEX_TABLE));
    }

    Table openEdgeLabelTable() throws IOException {
	return connection.getTable(TableName.valueOf(EDGE_LABELS_INDEX_TABLE));
    }

    final long createVertexId() {
	if (vertexIdCounter >= ID_CACHE) {
	    try (Table metaDataTable = openMetaDataTable()) {
		nextVertexId = metaDataTable.incrementColumnValue(ID_ROW_BYTES, METADATA_COLUMN_FAMILIY_BYTES,
			VERTEXID_COLUMN_BYTES, ID_CACHE);
		vertexIdCounter = 0;
	    } catch (IOException e) {
		throw new DuctileDBException("Could not create vertex id.", e);
	    }
	}
	long id = nextVertexId;
	++nextVertexId;
	++vertexIdCounter;
	return id;
    }

    final long createEdgeId() {
	if (edgeIdCounter >= ID_CACHE) {
	    try (Table metaDataTable = openMetaDataTable()) {
		nextEdgeId = metaDataTable.incrementColumnValue(ID_ROW_BYTES, METADATA_COLUMN_FAMILIY_BYTES,
			EDGEID_COLUMN_BYTES, ID_CACHE);
		edgeIdCounter = 0;
	    } catch (IOException e) {
		throw new DuctileDBException("Could not create edge id.", e);
	    }
	}
	long id = nextEdgeId;
	++nextEdgeId;
	++edgeIdCounter;
	return id;
    }

    @Override
    public DuctileDBVertex addVertex(Set<String> labels, Map<String, Object> properties) {
	long vertexId = createVertexId();
	tableOperations.add(new AddVertexOperation(connection, vertexId, labels, properties));
	properties.put(DUCTILEDB_ID_PROPERTY, vertexId);
	DuctileDBVertexImpl vertex = new DuctileDBVertexImpl(graph, vertexId, labels, properties, new ArrayList<>());
	return vertex;
    }

    @Override
    public DuctileDBEdge addEdge(DuctileDBVertex startVertex, DuctileDBVertex targetVertex, String label,
	    Map<String, Object> properties) {
	long edgeId = createEdgeId();
	tableOperations.add(
		new AddEdgeOperation(connection, edgeId, startVertex.getId(), targetVertex.getId(), label, properties));
	DuctileDBEdgeImpl edge = new DuctileDBEdgeImpl(graph, edgeId, label, startVertex, targetVertex,
		new HashMap<>());
	((DuctileDBVertexImpl) targetVertex).addEdge(edge);
	return edge;
    }

    @Override
    public DuctileDBEdge getEdge(long edgeId) {
	if (wasEdgeRemoved(edgeId)) {
	    return null;
	}
	DuctileDBEdge edge = addedEdge(edgeId);
	if (edge != null) {
	    return updateEdgeResult(edge);
	}
	try (Table table = openEdgeTable()) {
	    byte[] id = IdEncoder.encodeRowId(edgeId);
	    Get get = new Get(id);
	    Result result = table.get(get);
	    if (!result.isEmpty()) {
		edge = ResultDecoder.toEdge(graph, edgeId, result);
	    }
	    return updateEdgeResult(edge);
	} catch (IOException e) {
	    throw new DuctileDBException("Could not get edge.", e);
	}
    }

    @Override
    public Iterable<DuctileDBEdge> getEdges() {
	try (Table table = openEdgeTable()) {
	    ResultScanner result = table.getScanner(new Scan());
	    return new EdgeIterable(graph, this, result);
	} catch (IOException e) {
	    throw new DuctileDBException("Could not get edge.", e);
	}
    }

    @Override
    public Iterable<DuctileDBEdge> getEdges(String propertyKey, Object propertyValue) {
	if ((propertyKey == null) || (propertyKey.isEmpty())) {
	    throw new IllegalArgumentException("Property key must not be null.");
	}
	try (Table table = openEdgePropertyTable()) {
	    Result result = table.get(new Get(Bytes.toBytes(propertyKey)));
	    NavigableMap<byte[], byte[]> map = result.getFamilyMap(INDEX_COLUMN_FAMILY_BYTES);
	    List<DuctileDBEdge> edges = new ArrayList<>();
	    for (Entry<byte[], byte[]> entry : map.entrySet()) {
		Object value = SerializationUtils.deserialize(entry.getValue());
		if ((propertyValue == null) || (value.equals(propertyValue))) {
		    long edgeId = IdEncoder.decodeRowId(entry.getKey());
		    if (!wasEdgeRemoved(edgeId)) {
			edges.add(updateEdgeResult(getEdge(edgeId)));
		    }
		}
	    }
	    return new Iterable<DuctileDBEdge>() {
		@Override
		public Iterator<DuctileDBEdge> iterator() {
		    return edges.iterator();
		}
	    };
	} catch (IOException e) {
	    throw new DuctileDBException("Could not get edges.", e);
	}
    }

    @Override
    public Iterable<DuctileDBEdge> getEdges(String label) {
	if ((label == null) || (label.isEmpty())) {
	    throw new IllegalArgumentException("Label must not be null.");
	}
	try (Table table = openEdgeLabelTable()) {
	    Result result = table.get(new Get(Bytes.toBytes(label)));
	    NavigableMap<byte[], byte[]> map = result.getFamilyMap(INDEX_COLUMN_FAMILY_BYTES);
	    List<DuctileDBEdge> edges = new ArrayList<>();
	    for (byte[] edgeIdBytes : map.keySet()) {
		long edgeId = IdEncoder.decodeRowId(edgeIdBytes);
		if (!wasEdgeRemoved(edgeId)) {
		    edges.add(updateEdgeResult(getEdge(edgeId)));
		}
	    }
	    return new Iterable<DuctileDBEdge>() {
		@Override
		public Iterator<DuctileDBEdge> iterator() {
		    return edges.iterator();
		}
	    };
	} catch (IOException e) {
	    throw new DuctileDBException("Could not get edges.", e);
	}
    }

    @Override
    public DuctileDBVertex getVertex(long vertexId) {
	if (wasVertexRemoved(vertexId)) {
	    return null;
	}
	DuctileDBVertex vertex = addedVertex(vertexId);
	if (vertex != null) {
	    return updateVertexResult(vertex);
	}
	try (Table vertexTable = openVertexTable()) {
	    byte[] id = IdEncoder.encodeRowId(vertexId);
	    Get get = new Get(id);
	    Result result = vertexTable.get(get);
	    if (!result.isEmpty()) {
		vertex = ResultDecoder.toVertex(graph, vertexId, result);
	    }
	    return updateVertexResult(vertex);
	} catch (IOException e) {
	    throw new DuctileDBException("Could not get vertex.", e);
	}
    }

    @Override
    public Iterable<DuctileDBVertex> getVertices() {
	try (Table table = openVertexTable()) {
	    ResultScanner result = table.getScanner(new Scan());
	    return new VertexIterable(graph, this, result);
	} catch (IOException e) {
	    throw new DuctileDBException("Could not get vertices.", e);
	}
    }

    @Override
    public Iterable<DuctileDBVertex> getVertices(String propertyKey, Object propertyValue) {
	if ((propertyKey == null) || (propertyKey.isEmpty())) {
	    throw new IllegalArgumentException("Property key must not be null.");
	}
	try (Table table = openVertexPropertyTable()) {
	    List<DuctileDBVertex> vertices = new ArrayList<>();
	    Get get = new Get(Bytes.toBytes(propertyKey));
	    get.addFamily(INDEX_COLUMN_FAMILY_BYTES);
	    Result result = table.get(get);
	    NavigableMap<byte[], byte[]> propertyMap = result.getFamilyMap(INDEX_COLUMN_FAMILY_BYTES);
	    if (propertyMap != null) {
		for (Entry<byte[], byte[]> entry : propertyMap.entrySet()) {
		    Object value = SerializationUtils.deserialize(entry.getValue());
		    if ((propertyValue == null) || (propertyValue.equals(value))) {
			long vertexId = IdEncoder.decodeRowId(entry.getKey());
			if (!wasVertexRemoved(vertexId)) {
			    DuctileDBVertex vertex = getVertex(vertexId);
			    if (vertex != null) {
				vertices.add(updateVertexResult(vertex));
			    }
			}
		    }
		}
	    }
	    return new Iterable<DuctileDBVertex>() {
		@Override
		public Iterator<DuctileDBVertex> iterator() {
		    return vertices.iterator();
		}
	    };
	} catch (IOException e) {
	    throw new DuctileDBException("Could not get vertices.", e);
	}
    }

    @Override
    public Iterable<DuctileDBVertex> getVertices(String label) {
	if ((label == null) || (label.isEmpty())) {
	    throw new IllegalArgumentException("Label must not be null.");
	}
	try (Table table = openVertexLabelTable()) {
	    List<DuctileDBVertex> vertices = new ArrayList<>();
	    Get get = new Get(Bytes.toBytes(label));
	    get.addFamily(INDEX_COLUMN_FAMILY_BYTES);
	    Result result = table.get(get);
	    NavigableMap<byte[], byte[]> propertyMap = result.getFamilyMap(INDEX_COLUMN_FAMILY_BYTES);
	    if (propertyMap != null) {
		for (byte[] vertexIdBytes : propertyMap.keySet()) {
		    long vertexId = IdEncoder.decodeRowId(vertexIdBytes);
		    if (!wasVertexRemoved(vertexId)) {
			vertices.add(updateVertexResult(getVertex(vertexId)));
		    }
		}
	    }
	    return new Iterable<DuctileDBVertex>() {
		@Override
		public Iterator<DuctileDBVertex> iterator() {
		    return vertices.iterator();
		}
	    };
	} catch (IOException e) {
	    throw new DuctileDBException("Could not get vertices.", e);
	}
    }

    @Override
    public void removeEdge(DuctileDBEdge edge) {
	long edgeId = edge.getId();
	tableOperations.add(new RemoveEdgeOperation(connection, edgeId, edge.getVertex(EdgeDirection.IN).getId(),
		edge.getVertex(EdgeDirection.OUT).getId(), edge.getLabel(), edge.getPropertyKeys()));
    }

    @Override
    public void removeVertex(DuctileDBVertex vertex) {
	long vertexId = vertex.getId();
	for (DuctileDBEdge edge : vertex.getEdges(EdgeDirection.BOTH)) {
	    removeEdge(edge);
	}
	for (String label : vertex.getLabels()) {
	    removeLabel(vertex, label);
	}
	for (String key : vertex.getPropertyKeys()) {
	    removeProperty(vertex, key);
	}
	tableOperations.add(new RemoveVertexOperation(connection, vertexId));
    }

    @Override
    public void addLabel(DuctileDBVertex vertex, String label) {
	tableOperations.add(new AddVertexLabelOperation(connection, vertex.getId(), label));
    }

    @Override
    public void removeLabel(DuctileDBVertex vertex, String label) {
	tableOperations.add(new RemoveVertexLabelOperation(connection, vertex.getId(), label));
    }

    @Override
    public void setProperty(DuctileDBVertex vertex, String key, Object value) {
	tableOperations.add(new SetVertexPropertyOperation(connection, vertex.getId(), key, value));
    }

    @Override
    public void removeProperty(DuctileDBVertex vertex, String key) {
	tableOperations.add(new RemoveVertexPropertyOperation(connection, vertex.getId(), key));
    }

    @Override
    public void setProperty(DuctileDBEdge edge, String key, Object value) {
	tableOperations.add(new SetEdgePropertyOperation(connection, edge.getId(), edge.getStartVertex().getId(),
		edge.getTargetVertex().getId(), edge.getLabel(), key, value));
    }

    @Override
    public void removeProperty(DuctileDBEdge edge, String key) {
	tableOperations.add(new RemoveEdgePropertyOperation(connection, edge.getId(), edge.getStartVertex().getId(),
		edge.getTargetVertex().getId(), edge.getLabel(), key));
    }

    public List<DuctileDBVertex> addedVertices() {
	List<DuctileDBVertex> vertices = new ArrayList<>();
	for (TxOperation operation : tableOperations) {
	    if (AddVertexOperation.class.isAssignableFrom(operation.getClass())) {
		AddVertexOperation addOperation = (AddVertexOperation) operation;
		vertices.add(new DuctileDBVertexImpl(graph, addOperation.getVertexId(), addOperation.getLabels(),
			addOperation.getProperties(), new ArrayList<>()));
	    }
	}
	return vertices;
    }

    public List<DuctileDBEdge> addedEdges() {
	List<DuctileDBEdge> edges = new ArrayList<>();
	for (TxOperation operation : tableOperations) {
	    if (AddEdgeOperation.class.isAssignableFrom(operation.getClass())) {
		AddEdgeOperation addOperation = (AddEdgeOperation) operation;
		edges.add(new DuctileDBEdgeImpl(graph, addOperation.getEdgeId(), addOperation.getLabel(),
			addOperation.getStartVertexId(), addOperation.getTargetVertexId(),
			addOperation.getProperties()));
	    }
	}
	return edges;
    }

    public boolean wasVertexRemoved(long vertexId) {
	for (TxOperation operation : tableOperations) {
	    if (RemoveVertexOperation.class.isAssignableFrom(operation.getClass())) {
		if (vertexId == ((RemoveVertexOperation) operation).getVertexId()) {
		    return true;
		}
	    }
	}
	return false;
    }

    public boolean wasEdgeRemoved(long edgeId) {
	for (TxOperation operation : tableOperations) {
	    if (RemoveEdgeOperation.class.isAssignableFrom(operation.getClass())) {
		if (edgeId == ((RemoveEdgeOperation) operation).getEdgeId()) {
		    return true;
		}
	    }
	}
	return false;
    }

    public DuctileDBVertex updateVertexResult(DuctileDBVertex vertex) {
	if (vertex == null) {
	    return null;
	}
	for (TxOperation operation : tableOperations) {
	    vertex = operation.updateVertex(vertex);
	}
	return vertex;
    }

    public DuctileDBEdge updateEdgeResult(DuctileDBEdge edge) {
	if (edge == null) {
	    return null;
	}
	for (TxOperation operation : tableOperations) {
	    edge = operation.updateEdge(edge);
	}
	return edge;
    }

    private DuctileDBVertex addedVertex(long vertexId) {
	for (TxOperation operation : tableOperations) {
	    if (AddVertexOperation.class.isAssignableFrom(operation.getClass())) {
		AddVertexOperation addOperation = (AddVertexOperation) operation;
		if (vertexId == addOperation.getVertexId()) {
		    return updateVertexResult(new DuctileDBVertexImpl(graph, addOperation.getVertexId(),
			    addOperation.getLabels(), addOperation.getProperties(), new ArrayList<>()));
		}
	    }
	}
	return null;
    }

    private DuctileDBEdge addedEdge(long edgeId) {
	for (TxOperation operation : tableOperations) {
	    if (AddEdgeOperation.class.isAssignableFrom(operation.getClass())) {
		AddEdgeOperation addOperation = (AddEdgeOperation) operation;
		if (edgeId == addOperation.getEdgeId()) {
		    return updateEdgeResult(new DuctileDBEdgeImpl(graph, addOperation.getEdgeId(),
			    addOperation.getLabel(), addOperation.getStartVertexId(), addOperation.getTargetVertexId(),
			    addOperation.getProperties()));
		}
	    }
	}
	return null;
    }
}
