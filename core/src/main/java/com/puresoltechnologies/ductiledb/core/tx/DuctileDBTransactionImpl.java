package com.puresoltechnologies.ductiledb.core.tx;

import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.EDGEID_COLUMN_BYTES;
import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.ID_ROW_BYTES;
import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.INDEX_COLUMN_FAMILY_BYTES;
import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.METADATA_COLUMN_FAMILIY_BYTES;
import static com.puresoltechnologies.ductiledb.core.schema.DuctileDBSchema.VERTEXID_COLUMN_BYTES;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.Consumer;

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
import com.puresoltechnologies.ductiledb.api.exceptions.DuctileDBException;
import com.puresoltechnologies.ductiledb.api.exceptions.GraphElementRemovedException;
import com.puresoltechnologies.ductiledb.api.exceptions.NoSuchGraphElementException;
import com.puresoltechnologies.ductiledb.api.tx.DuctileDBTransaction;
import com.puresoltechnologies.ductiledb.core.DuctileDBAttachedEdge;
import com.puresoltechnologies.ductiledb.core.DuctileDBAttachedVertex;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.core.schema.SchemaTable;
import com.puresoltechnologies.ductiledb.core.utils.ElementUtils;
import com.puresoltechnologies.ductiledb.core.utils.IdEncoder;
import com.puresoltechnologies.ductiledb.core.utils.Serializer;

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

    private final LinkedList<TxOperation> txOperations = new LinkedList<>();
    private final Map<Long, DuctileDBCacheVertex> vertexCache = new HashMap<>();
    private final Map<Long, DuctileDBCacheEdge> edgeCache = new HashMap<>();

    private final ThreadLocal<List<Consumer<Status>>> transactionListeners = ThreadLocal
	    .withInitial(() -> new ArrayList<>());

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
	    fireOnCommit();
	} catch (IOException e) {
	    throw new DuctileDBException("Could not cmomit changes.", e);
	} finally {
	    clear();
	}
    }

    @Override
    public void rollback() {
	checkForClosed();
	try {
	    txOperations.descendingIterator().forEachRemaining(operation -> operation.rollbackInternally());
	    fireOnRollback();
	} finally {
	    clear();
	}
    }

    @Override
    public boolean isOpen() {
	return (!vertexCache.isEmpty()) || (!edgeCache.isEmpty()) || (!txOperations.isEmpty());
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
	rollback();
	closed = true;
    }

    void setCachedVertex(DuctileDBCacheVertex vertex) {
	vertexCache.put(vertex.getId(), vertex);
    }

    DuctileDBCacheVertex getCachedVertex(long vertexId)
	    throws GraphElementRemovedException, NoSuchGraphElementException {
	if (wasVertexRemoved(vertexId)) {
	    throw new GraphElementRemovedException("Vertex with id '" + vertexId + "' was already removed.");
	}
	DuctileDBCacheVertex vertex = vertexCache.get(vertexId);
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
	    if (vertex == null) {
		throw new NoSuchGraphElementException("vertex with id '" + vertexId + "' does not exist.");
	    }
	    setCachedVertex(vertex);
	    return vertex;
	} catch (IOException e) {
	    throw new DuctileDBException("Could not get vertex.", e);
	}
    }

    void removeCachedVertex(long vertexId) {
	vertexCache.put(vertexId, null);
    }

    boolean wasVertexRemoved(long vertexId) {
	return (vertexCache.containsKey(vertexId)) && (vertexCache.get(vertexId) == null);
    }

    void setCachedEdge(DuctileDBCacheEdge edge) {
	edgeCache.put(edge.getId(), edge);
    }

    DuctileDBCacheEdge getCachedEdge(long edgeId) throws GraphElementRemovedException, NoSuchGraphElementException {
	if (wasEdgeRemoved(edgeId)) {
	    throw new GraphElementRemovedException("Edge with id '" + edgeId + "' was already removed.");
	}
	DuctileDBCacheEdge edge = edgeCache.get(edgeId);
	if (edge != null) {
	    return edge;
	}
	try (Table table = openEdgeTable()) {
	    byte[] id = IdEncoder.encodeRowId(edgeId);
	    Get get = new Get(id);
	    Result result = table.get(get);
	    if (!result.isEmpty()) {
		edge = ResultDecoder.toCacheEdge(graph, edgeId, result);
	    }
	    if (edge == null) {
		throw new NoSuchGraphElementException("Edge with id '" + edgeId + "' does not exist.");
	    }
	    setCachedEdge(edge);
	    return edge;
	} catch (IOException e) {
	    throw new DuctileDBException("Could not get edge.", e);
	}
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

    Table openVertexTypesTable() throws IOException {
	return connection.getTable(SchemaTable.VERTEX_TYPES.getTableName());
    }

    Table openEdgePropertyTable() throws IOException {
	return connection.getTable(SchemaTable.EDGE_PROPERTIES.getTableName());
    }

    Table openEdgeTypesTable() throws IOException {
	return connection.getTable(SchemaTable.EDGE_TYPES.getTableName());
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
    public DuctileDBVertex addVertex(Set<String> types, Map<String, Object> properties) {
	long vertexId = createVertexId();
	addTxOperation(new AddVertexOperation(this, vertexId, types, properties));
	return getVertex(vertexId);
    }

    @Override
    public DuctileDBEdge addEdge(DuctileDBVertex startVertex, DuctileDBVertex targetVertex, String type,
	    Map<String, Object> properties) {
	long edgeId = createEdgeId();
	addTxOperation(new AddEdgeOperation(this, edgeId, startVertex.getId(), targetVertex.getId(), type, properties));
	return getEdge(edgeId);
    }

    @Override
    public DuctileDBEdge getEdge(long edgeId) {
	getCachedEdge(edgeId);
	return new DuctileDBAttachedEdge(graph, edgeId);
    }

    @Override
    public Iterable<DuctileDBEdge> getEdges() {
	try (Table table = openEdgeTable()) {
	    ResultScanner result = table.getScanner(new Scan());
	    return new AttachedEdgeIterable(graph, this, result);
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
		Object value = Serializer.deserialize(entry.getValue());
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
    public Iterable<DuctileDBEdge> getEdges(String type) {
	if ((type == null) || (type.isEmpty())) {
	    throw new IllegalArgumentException("Type must not be null.");
	}
	try (Table table = openEdgeTypesTable()) {
	    Result result = table.get(new Get(Bytes.toBytes(type)));
	    NavigableMap<byte[], byte[]> map = result.getFamilyMap(INDEX_COLUMN_FAMILY_BYTES);
	    List<DuctileDBEdge> edges = new ArrayList<>();
	    for (byte[] edgeIdBytes : map.keySet()) {
		long edgeId = IdEncoder.decodeRowId(edgeIdBytes);
		if (!wasEdgeRemoved(edgeId)) {
		    DuctileDBEdge edge = getEdge(edgeId);
		    if ((edge != null) && (type.equals(edge.getType()))) {
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
	getCachedVertex(vertexId);
	return new DuctileDBAttachedVertex(graph, vertexId);
    }

    @Override
    public Iterable<DuctileDBVertex> getVertices() {
	try (Table table = openVertexTable()) {
	    ResultScanner result = table.getScanner(new Scan());
	    return new AttachedVertexIterable(graph, this, result);
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
		    Object value = Serializer.deserialize(entry.getValue());
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
    public Iterable<DuctileDBVertex> getVertices(String type) {
	if ((type == null) || (type.isEmpty())) {
	    throw new IllegalArgumentException("Type must not be null.");
	}
	try (Table table = openVertexTypesTable()) {
	    List<DuctileDBVertex> vertices = new ArrayList<>();
	    Get get = new Get(Bytes.toBytes(type));
	    get.addFamily(INDEX_COLUMN_FAMILY_BYTES);
	    Result result = table.get(get);
	    NavigableMap<byte[], byte[]> propertyMap = result.getFamilyMap(INDEX_COLUMN_FAMILY_BYTES);
	    if (propertyMap != null) {
		for (byte[] vertexIdBytes : propertyMap.keySet()) {
		    long vertexId = IdEncoder.decodeRowId(vertexIdBytes);
		    if (!wasVertexRemoved(vertexId)) {
			DuctileDBVertex vertex = getVertex(vertexId);
			if ((vertex != null) && (ElementUtils.getTypes(vertex).contains(type))) {
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
	if (wasEdgeRemoved(edge.getId())) {
	    return;
	}
	addTxOperation(new RemoveEdgeOperation(this, edge));
    }

    @Override
    public void removeVertex(DuctileDBVertex vertex) {
	if (wasVertexRemoved(vertex.getId())) {
	    return;
	}
	long vertexId = vertex.getId();
	for (DuctileDBEdge edge : vertex.getEdges(EdgeDirection.BOTH)) {
	    removeEdge(edge);
	}
	for (String type : ElementUtils.getTypes(vertex)) {
	    removeType(vertex, type);
	}
	for (String key : new HashSet<>(vertex.getPropertyKeys())) {
	    removeProperty(vertex, key);
	}
	addTxOperation(new RemoveVertexOperation(this, vertexId));
    }

    public void addType(DuctileDBVertex vertex, String type) {
	addTxOperation(new AddVertexTypeOperation(this, vertex.getId(), type));
    }

    public void removeType(DuctileDBVertex vertex, String type) {
	addTxOperation(new RemoveVertexTypeOperation(this, vertex, type));
    }

    public void setProperty(DuctileDBVertex vertex, String key, Object value) {
	addTxOperation(new SetVertexPropertyOperation(this, vertex, key, value));
    }

    public void removeProperty(DuctileDBVertex vertex, String key) {
	addTxOperation(new RemoveVertexPropertyOperation(this, vertex, key));
    }

    public void setProperty(DuctileDBEdge edge, String key, Object value) {
	addTxOperation(new SetEdgePropertyOperation(this, edge, key, value));
    }

    public void removeProperty(DuctileDBEdge edge, String key) {
	addTxOperation(new RemoveEdgePropertyOperation(this, edge, key));
    }

    private void addTxOperation(TxOperation operation) {
	operation.commitInternally();
	txOperations.add(operation);
    }

    public List<DuctileDBCacheVertex> addedVertices() {
	List<DuctileDBCacheVertex> vertices = new ArrayList<>();
	for (TxOperation operation : txOperations) {
	    if (AddVertexOperation.class.isAssignableFrom(operation.getClass())) {
		AddVertexOperation addOperation = (AddVertexOperation) operation;
		vertices.add(getCachedVertex(addOperation.getVertexId()));
	    }
	}
	return vertices;
    }

    public List<DuctileDBCacheEdge> addedEdges() {
	List<DuctileDBCacheEdge> edges = new ArrayList<>();
	for (TxOperation operation : txOperations) {
	    if (AddEdgeOperation.class.isAssignableFrom(operation.getClass())) {
		AddEdgeOperation addOperation = (AddEdgeOperation) operation;
		edges.add(getCachedEdge(addOperation.getEdgeId()));
	    }
	}
	return edges;
    }

    private void fireOnCommit() {
	transactionListeners.get().forEach(c -> c.accept(Status.COMMIT));
    }

    private void fireOnRollback() {
	transactionListeners.get().forEach(c -> c.accept(Status.ROLLBACK));
    }

    @Override
    public void addTransactionListener(Consumer<Status> listener) {
	transactionListeners.get().add(listener);
    }

    @Override
    public void removeTransactionListener(final Consumer<Status> listener) {
	transactionListeners.get().remove(listener);
    }

    @Override
    public void clearTransactionListeners() {
	transactionListeners.get().clear();
    }

    public List<DuctileDBEdge> getEdges(long vertexId, EdgeDirection direction, String[] edgeTypes) {
	DuctileDBCacheVertex cachedVertex = getCachedVertex(vertexId);
	List<DuctileDBEdge> edges = new ArrayList<>();
	List<String> typeList = Arrays.asList(edgeTypes);
	for (DuctileDBEdge cachedEdge : cachedVertex.getEdges(direction, edgeTypes)) {
	    if ((edgeTypes.length == 0) || (typeList.contains(cachedEdge.getType()))) {
		DuctileDBAttachedEdge edge = ElementUtils.toAttached(cachedEdge);
		switch (direction) {
		case IN:
		    if (edge.getTargetVertex().getId() == vertexId) {
			edges.add(ElementUtils.toAttached(edge));
		    }
		    break;
		case OUT:
		    if (edge.getStartVertex().getId() == vertexId) {
			edges.add(ElementUtils.toAttached(edge));
		    }
		    break;
		case BOTH:
		    edges.add(ElementUtils.toAttached(edge));
		    break;
		default:
		    throw new IllegalArgumentException("Direction '" + direction + "' is not supported.");
		}
	    }
	}
	return edges;
    }

    public Iterable<DuctileDBEdge> getVertexEdges(long vertexId) {
	Iterable<DuctileDBEdge> cachedEdges = getCachedVertex(vertexId).getEdges(EdgeDirection.BOTH);
	List<DuctileDBEdge> edges = new ArrayList<>();
	for (DuctileDBEdge edge : cachedEdges) {
	    if (!wasEdgeRemoved(edge.getId())) {
		edges.add(edge);
	    }
	}
	return edges;
    }

    public Iterable<String> getVertexTypes(long vertexId) {
	DuctileDBCacheVertex cachedVertex = getCachedVertex(vertexId);
	return cachedVertex.getTypes();
    }

    public boolean hasType(long vertexId, String type) {
	DuctileDBCacheVertex cachedVertex = getCachedVertex(vertexId);
	return cachedVertex.hasType(type);
    }

    public Set<String> getVertexPropertyKeys(long vertexId) {
	DuctileDBCacheVertex cachedVertex = getCachedVertex(vertexId);
	return cachedVertex.getPropertyKeys();
    }

    public <T> T getVertexProperty(long vertexId, String key) {
	DuctileDBCacheVertex cachedVertex = getCachedVertex(vertexId);
	return cachedVertex.getProperty(key);
    }

    public DuctileDBVertex getEdgeStartVertex(long edgeId) {
	DuctileDBCacheEdge cachedEdge = getCachedEdge(edgeId);
	return getVertex(cachedEdge.getStartVertexId());
    }

    public DuctileDBVertex getEdgeTargetVertex(long edgeId) {
	DuctileDBCacheEdge cachedEdge = getCachedEdge(edgeId);
	return getVertex(cachedEdge.getTargetVertexId());
    }

    public String getEdgeType(long edgeId) {
	DuctileDBCacheEdge cachedEdge = getCachedEdge(edgeId);
	return cachedEdge.getType();
    }

    public Set<String> getEdgePropertyKeys(long edgeId) {
	DuctileDBCacheEdge cachedEdge = getCachedEdge(edgeId);
	return cachedEdge.getPropertyKeys();
    }

    public <T> T getEdgeProperty(long edgeId, String key) {
	DuctileDBCacheEdge cachedEdge = getCachedEdge(edgeId);
	return cachedEdge.getProperty(key);
    }

}
