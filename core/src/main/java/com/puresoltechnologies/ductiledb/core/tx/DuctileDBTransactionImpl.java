package com.puresoltechnologies.ductiledb.core.tx;

import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.DUCTILEDB_CREATE_TIMESTAMP_PROPERTY;
import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.DUCTILEDB_ID_PROPERTY;
import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.EDGEID_COLUMN_BYTES;
import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.ID_ROW_BYTES;
import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.INDEX_COLUMN_FAMILY_BYTES;
import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.METADATA_COLUMN_FAMILIY_BYTES;
import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.VERTEXID_COLUMN_BYTES;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.commons.lang.SerializationUtils;
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
import com.puresoltechnologies.ductiledb.core.ResultDecoder;
import com.puresoltechnologies.ductiledb.core.schema.SchemaTable;
import com.puresoltechnologies.ductiledb.core.utils.ElementUtils;
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

    private final List<TxOperation> txOperations = new ArrayList<>();
    private final Map<Long, DuctileDBVertex> vertexCache = new HashMap<>();
    private final Map<Long, DuctileDBEdge> edgeCache = new HashMap<>();

    private final DuctileDBGraphImpl graph;
    private final Connection connection;
    private boolean closed = false;

    public DuctileDBTransactionImpl(DuctileDBGraphImpl graph) {
	this.graph = graph;
	this.connection = graph.getConnection();

    }

    public Connection getConnection() {
	return connection;
    }

    public DuctileDBGraphImpl getGraph() {
	return graph;
    }

    @Override
    public void commit() {
	checkForClosed();
	try {
	    for (TxOperation operation : txOperations) {
		operation.perform();
	    }
	    clear();
	} catch (IOException e) {
	    throw new DuctileDBException("Could not cmomit changes.", e);
	}
    }

    @Override
    public void rollback() {
	checkForClosed();
	clear();
    }

    private void clear() {
	txOperations.clear();
	vertexCache.clear();
	edgeCache.clear();
    }

    private void checkForClosed() {
	if (closed) {
	    throw new IllegalStateException("Transaction was already closed.");
	}
    }

    @Override
    public void close() {
	txOperations.clear();
	closed = true;
    }

    void setCachedVertex(DuctileDBVertex vertex) {
	vertexCache.put(vertex.getId(), vertex);
    }

    DuctileDBVertex getCachedVertex(long vertexId) {
	return vertexCache.get(vertexId);
    }

    void removeCachedVertex(long vertexId) {
	vertexCache.put(vertexId, null);
    }

    boolean wasVertexRemoved(long vertexId) {
	return (vertexCache.containsKey(vertexId)) && (vertexCache.get(vertexId) == null);
    }

    void setCachedEdge(DuctileDBEdge edge) {
	edgeCache.put(edge.getId(), edge);
    }

    DuctileDBEdge getCachedEdge(long edgeId) {
	return edgeCache.get(edgeId);
    }

    void removeCachedEdge(long edgeId) {
	edgeCache.put(edgeId, null);
    }

    boolean wasEdgeRemoved(long edgeId) {
	return (edgeCache.containsKey(edgeId)) && (edgeCache.get(edgeId) == null);
    }

    Table openMetaDataTable() throws IOException {
	return connection.getTable(SchemaTable.METADATA.getTableName());
    }

    Table openVertexTable() throws IOException {
	return connection.getTable(SchemaTable.VERTICES.getTableName());
    }

    Table openEdgeTable() throws IOException {
	return connection.getTable(SchemaTable.EDGES.getTableName());
    }

    Table openVertexPropertyTable() throws IOException {
	return connection.getTable(SchemaTable.VERTEX_PROPERTIES.getTableName());
    }

    Table openVertexLabelTable() throws IOException {
	return connection.getTable(SchemaTable.VERTEX_LABELS.getTableName());
    }

    Table openEdgePropertyTable() throws IOException {
	return connection.getTable(SchemaTable.EDGE_PROPERTIES.getTableName());
    }

    Table openEdgeLabelTable() throws IOException {
	return connection.getTable(SchemaTable.EDGE_LABELS.getTableName());
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
	DuctileDBVertexImpl vertex = new DuctileDBVertexImpl(graph, vertexId, labels, new HashMap<>(properties),
		new ArrayList<>());
	properties.put(DUCTILEDB_ID_PROPERTY, vertexId);
	properties.put(DUCTILEDB_CREATE_TIMESTAMP_PROPERTY, new Date());
	txOperations.add(new AddVertexOperation(this, vertexId, labels, properties));
	return vertex;
    }

    @Override
    public DuctileDBEdge addEdge(DuctileDBVertex startVertex, DuctileDBVertex targetVertex, String label,
	    Map<String, Object> properties) {
	long edgeId = createEdgeId();
	DuctileDBEdgeImpl edge = new DuctileDBEdgeImpl(graph, edgeId, label, startVertex, targetVertex,
		new HashMap<>(properties));
	properties.put(DUCTILEDB_CREATE_TIMESTAMP_PROPERTY, new Date());
	txOperations
		.add(new AddEdgeOperation(this, edgeId, startVertex.getId(), targetVertex.getId(), label, properties));
	((DuctileDBVertexImpl) startVertex).addEdge(edge);
	((DuctileDBVertexImpl) targetVertex).addEdge(edge);
	return edge;
    }

    @Override
    public DuctileDBEdge getEdge(long edgeId) {
	if (wasEdgeRemoved(edgeId)) {
	    return null;
	}
	DuctileDBEdge edge = getCachedEdge(edgeId);
	if (edge != null) {
	    return edge;
	}
	try (Table table = openEdgeTable()) {
	    byte[] id = IdEncoder.encodeRowId(edgeId);
	    Get get = new Get(id);
	    Result result = table.get(get);
	    if (!result.isEmpty()) {
		edge = ResultDecoder.toEdge(graph, edgeId, result);
	    }
	    if (edge != null) {
		setCachedEdge(edge.clone());
	    }
	    return edge;
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
			DuctileDBEdge edge = getEdge(edgeId);
			if ((edge != null) && //
				((propertyValue == null) || (propertyValue.equals(edge.getProperty(propertyKey))))) {
			    edges.add(edge);
			}
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
		    DuctileDBEdge edge = getEdge(edgeId);
		    if ((edge != null) && (label.equals(edge.getLabel()))) {
			edges.add(edge);
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
    public DuctileDBVertex getVertex(long vertexId) {
	if (wasVertexRemoved(vertexId)) {
	    return null;
	}
	DuctileDBVertex vertex = getCachedVertex(vertexId);
	if (vertex != null) {
	    return vertex;
	}
	try (Table vertexTable = openVertexTable()) {
	    byte[] id = IdEncoder.encodeRowId(vertexId);
	    Get get = new Get(id);
	    Result result = vertexTable.get(get);
	    if (!result.isEmpty()) {
		vertex = ResultDecoder.toVertex(graph, vertexId, result);
	    }
	    if (vertex != null) {
		setCachedVertex(vertex.clone());
	    }
	    return vertex;
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
			    if ((vertex != null) && //
				    ((propertyValue == null)
					    || (vertex.getProperty(propertyKey).equals(propertyValue)))) {
				vertices.add(vertex);
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
			DuctileDBVertex vertex = getVertex(vertexId);
			if ((vertex != null) && (ElementUtils.getLabels(vertex).contains(label))) {
			    vertices.add(vertex);
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
    public void removeEdge(DuctileDBEdge edge) {
	txOperations.add(new RemoveEdgeOperation(this, edge));
	DuctileDBVertex startVertex = edge.getStartVertex();
	if (startVertex != null) {
	    ((DuctileDBVertexImpl) startVertex).removeEdge(edge);
	}
	DuctileDBVertex targetVertex = edge.getTargetVertex();
	if (targetVertex != null) {
	    ((DuctileDBVertexImpl) targetVertex).removeEdge(edge);
	}
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
	txOperations.add(new RemoveVertexOperation(this, vertexId));
    }

    @Override
    public void addLabel(DuctileDBVertex vertex, String label) {
	txOperations.add(new AddVertexLabelOperation(this, vertex, label));
    }

    @Override
    public void removeLabel(DuctileDBVertex vertex, String label) {
	txOperations.add(new RemoveVertexLabelOperation(this, vertex, label));
    }

    @Override
    public void setProperty(DuctileDBVertex vertex, String key, Object value) {
	txOperations.add(new SetVertexPropertyOperation(this, vertex, key, value));
    }

    @Override
    public void removeProperty(DuctileDBVertex vertex, String key) {
	txOperations.add(new RemoveVertexPropertyOperation(this, vertex, key));
    }

    @Override
    public void setProperty(DuctileDBEdge edge, String key, Object value) {
	txOperations.add(new SetEdgePropertyOperation(this, edge, key, value));
    }

    @Override
    public void removeProperty(DuctileDBEdge edge, String key) {
	txOperations.add(new RemoveEdgePropertyOperation(this, edge, key));
    }

    public List<DuctileDBVertex> addedVertices() {
	List<DuctileDBVertex> vertices = new ArrayList<>();
	for (TxOperation operation : txOperations) {
	    if (AddVertexOperation.class.isAssignableFrom(operation.getClass())) {
		AddVertexOperation addOperation = (AddVertexOperation) operation;
		vertices.add(getCachedVertex(addOperation.getVertexId()));
	    }
	}
	return vertices;
    }

    public List<DuctileDBEdge> addedEdges() {
	List<DuctileDBEdge> edges = new ArrayList<>();
	for (TxOperation operation : txOperations) {
	    if (AddEdgeOperation.class.isAssignableFrom(operation.getClass())) {
		AddEdgeOperation addOperation = (AddEdgeOperation) operation;
		edges.add(getCachedEdge(addOperation.getEdgeId()));
	    }
	}
	return edges;
    }
}
