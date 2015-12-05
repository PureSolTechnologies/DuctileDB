package com.puresoltechnologies.ductiledb.core.tx;

import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.DUCTILEDB_ID_PROPERTY;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.EDGEID_COLUMN_BYTES;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.EDGES_COLUMN_FAMILY_BYTES;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.EDGES_TABLE;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.EDGE_LABELS_INDEX_TABLE;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.EDGE_PROPERTIES_INDEX_TABLE;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.ID_ROW_BYTES;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.INDEX_COLUMN_FAMILY_BYTES;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.LABELS_COLUMN_FAMILIY_BYTES;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.METADATA_COLUMN_FAMILIY_BYTES;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.METADATA_TABLE;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.PROPERTIES_COLUMN_FAMILY_BYTES;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.START_VERTEXID_COLUMN_BYTES;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.TARGET_VERTEXID_COLUMN_BYTES;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.VERICES_COLUMN_FAMILY_BYTES;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.VERTEXID_COLUMN_BYTES;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.VERTEX_LABELS_INDEX_TABLE;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.VERTEX_PROPERTIES_INDEX_TABLE;
import static com.puresoltechnologies.ductiledb.core.DuctileDBSchema.VERTICES_TABLE;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
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
import com.puresoltechnologies.ductiledb.core.EdgeKey;
import com.puresoltechnologies.ductiledb.core.EdgeValue;
import com.puresoltechnologies.ductiledb.core.ResultDecoder;
import com.puresoltechnologies.ductiledb.core.VertexIterable;
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

    public final Map<TableName, List<DuctileDBOperation>> tableOperations = new HashMap<>();
    private final DuctileDBGraphImpl graph;
    private final Connection connection;
    private boolean closed = false;

    public DuctileDBTransactionImpl(DuctileDBGraphImpl graph) {
	this.graph = graph;
	this.connection = graph.getConnection();

    }

    private void put(String tableName, Put put) {
	checkForClosed();
	TableName table = TableName.valueOf(tableName);
	List<DuctileDBOperation> operationList = tableOperations.get(table);
	if (operationList == null) {
	    operationList = new ArrayList<>();
	    tableOperations.put(table, operationList);
	}
	operationList.add(new DuctileDBOperation(put));
    }

    private void put(String tableName, List<Put> puts) {
	checkForClosed();
	if (puts.isEmpty()) {
	    return;
	}
	TableName table = TableName.valueOf(tableName);
	List<DuctileDBOperation> operationList = tableOperations.get(table);
	if (operationList == null) {
	    operationList = new ArrayList<>();
	    tableOperations.put(table, operationList);
	}
	for (Put put : puts) {
	    operationList.add(new DuctileDBOperation(put));
	}
    }

    private void delete(String tableName, Delete delete) {
	checkForClosed();
	TableName table = TableName.valueOf(tableName);
	List<DuctileDBOperation> operationList = tableOperations.get(table);
	if (operationList == null) {
	    operationList = new ArrayList<>();
	    tableOperations.put(table, operationList);
	}
	operationList.add(new DuctileDBOperation(delete));
    }

    private void delete(String tableName, List<Delete> deletes) {
	checkForClosed();
	if (deletes.isEmpty()) {
	    return;
	}
	TableName table = TableName.valueOf(tableName);
	List<DuctileDBOperation> operationList = tableOperations.get(table);
	if (operationList == null) {
	    operationList = new ArrayList<>();
	    tableOperations.put(table, operationList);
	}
	for (Delete delete : deletes) {
	    operationList.add(new DuctileDBOperation(delete));
	}
    }

    @Override
    public void commit() {
	checkForClosed();
	try {
	    for (Entry<TableName, List<DuctileDBOperation>> entry : tableOperations.entrySet()) {
		mutateTable(entry.getKey(), entry.getValue());
	    }
	    tableOperations.clear();
	} catch (IOException e) {
	    throw new DuctileDBException("Could not cmomit changes.", e);
	}
    }

    private void mutateTable(TableName tableName, List<DuctileDBOperation> operations) throws IOException {
	try (Table table = connection.getTable(tableName)) {
	    List<Put> puts = new ArrayList<>();
	    List<Delete> deletes = new ArrayList<>();
	    OperationType currentOperationType = null;
	    for (DuctileDBOperation operation : operations) {
		if (currentOperationType != operation.getOperationType()) {
		    if (currentOperationType != null) {
			switch (currentOperationType) {
			case PUT:
			    table.put(puts);
			    puts.clear();
			    break;
			case DELETE:
			    table.delete(deletes);
			    deletes.clear();
			    break;
			default:
			    throw new DuctileDBException(
				    "Operation type '" + currentOperationType + "' is not implemented.");
			}
		    }
		    currentOperationType = operation.getOperationType();
		}
		switch (currentOperationType) {
		case PUT:
		    puts.add(operation.getPut());
		    break;
		case DELETE:
		    deletes.add(operation.getDelete());
		    break;
		default:
		    throw new DuctileDBException("Operation type '" + currentOperationType + "' is not implemented.");
		}
	    }
	    switch (currentOperationType) {
	    case PUT:
		table.put(puts);
		break;
	    case DELETE:
		table.delete(deletes);
		break;
	    default:
		throw new DuctileDBException("Operation type '" + currentOperationType + "' is not implemented.");
	    }
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

    private Put createVertexLabelIndexPut(long vertexId, String label) {
	Put labelIndexPut = new Put(Bytes.toBytes(label));
	labelIndexPut.addColumn(INDEX_COLUMN_FAMILY_BYTES, IdEncoder.encodeRowId(vertexId), new byte[0]);
	return labelIndexPut;
    }

    private Put createVertexPropertyIndexPut(long vertexId, String key, Object value) {
	Put indexPut = new Put(Bytes.toBytes(key));
	indexPut.addColumn(INDEX_COLUMN_FAMILY_BYTES, IdEncoder.encodeRowId(vertexId),
		SerializationUtils.serialize((Serializable) value));
	return indexPut;
    }

    private Delete createVertexLabelIndexDelete(long vertexId, String label) {
	Delete labelIndexPut = new Delete(Bytes.toBytes(label));
	labelIndexPut.addColumn(INDEX_COLUMN_FAMILY_BYTES, IdEncoder.encodeRowId(vertexId));
	return labelIndexPut;
    }

    private Delete createVertexPropertyIndexDelete(long vertexId, String key) {
	Delete indexPut = new Delete(Bytes.toBytes(key));
	indexPut.addColumn(INDEX_COLUMN_FAMILY_BYTES, IdEncoder.encodeRowId(vertexId));
	return indexPut;
    }

    private Put createEdgeLabelIndexPut(long edgeId, String edgeType) {
	Put labelIndexPut = new Put(Bytes.toBytes(edgeType));
	labelIndexPut.addColumn(INDEX_COLUMN_FAMILY_BYTES, IdEncoder.encodeRowId(edgeId), new byte[0]);
	return labelIndexPut;
    }

    private Put createEdgePropertyIndexPut(long edgeId, String key, Object value) {
	Put indexPut = new Put(Bytes.toBytes(key));
	indexPut.addColumn(INDEX_COLUMN_FAMILY_BYTES, IdEncoder.encodeRowId(edgeId),
		SerializationUtils.serialize((Serializable) value));
	return indexPut;
    }

    private Delete createEdgeLabelIndexDelete(long edgeId, String edgeType) {
	Delete labelIndexPut = new Delete(Bytes.toBytes(edgeType));
	labelIndexPut.addColumn(INDEX_COLUMN_FAMILY_BYTES, IdEncoder.encodeRowId(edgeId));
	return labelIndexPut;
    }

    private Delete createEdgePropertyIndexDelete(long edgeId, String key) {
	Delete indexPut = new Delete(Bytes.toBytes(key));
	indexPut.addColumn(INDEX_COLUMN_FAMILY_BYTES, IdEncoder.encodeRowId(edgeId));
	return indexPut;
    }

    @Override
    public DuctileDBVertex addVertex() {
	return addVertex(new HashSet<>(), new HashMap<>());
    }

    @Override
    public DuctileDBVertex addVertex(Set<String> labels, Map<String, Object> properties) {
	long vertexId = createVertexId();
	byte[] id = IdEncoder.encodeRowId(vertexId);
	Put put = new Put(id);
	List<Put> labelIndex = new ArrayList<>();
	for (String label : labels) {
	    put.addColumn(LABELS_COLUMN_FAMILIY_BYTES, Bytes.toBytes(label), new byte[0]);
	    labelIndex.add(createVertexLabelIndexPut(vertexId, label));
	}
	List<Put> propertyIndex = new ArrayList<>();
	properties.put(DUCTILEDB_ID_PROPERTY, vertexId);
	for (Entry<String, Object> property : properties.entrySet()) {
	    String key = property.getKey();
	    Object value = property.getValue();
	    put.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, Bytes.toBytes(key),
		    SerializationUtils.serialize((Serializable) value));
	    propertyIndex.add(createVertexPropertyIndexPut(vertexId, property.getKey(), property.getValue()));
	}
	put(VERTICES_TABLE, put);
	put(VERTEX_LABELS_INDEX_TABLE, labelIndex);
	put(VERTEX_PROPERTIES_INDEX_TABLE, propertyIndex);
	return new DuctileDBVertexImpl(graph, vertexId, labels, properties, new ArrayList<>());
    }

    @Override
    public DuctileDBEdge addEdge(DuctileDBVertex startVertex, DuctileDBVertex targetVertex, String edgeType) {
	return addEdge(startVertex, targetVertex, edgeType, new HashMap<>());
    }

    @Override
    public DuctileDBEdge addEdge(DuctileDBVertex startVertex, DuctileDBVertex targetVertex, String edgeType,
	    Map<String, Object> properties) {
	long edgeId = createEdgeId();
	byte[] edgeValue = new EdgeValue(properties).encode();
	// Put to Start Vertex
	Put outPut = new Put(IdEncoder.encodeRowId(startVertex.getId()));
	outPut.addColumn(EDGES_COLUMN_FAMILY_BYTES,
		new EdgeKey(EdgeDirection.OUT, edgeId, targetVertex.getId(), edgeType).encode(), edgeValue);
	// Put to Target Vertex
	Put inPut = new Put(IdEncoder.encodeRowId(targetVertex.getId()));
	inPut.addColumn(EDGES_COLUMN_FAMILY_BYTES,
		new EdgeKey(EdgeDirection.IN, edgeId, startVertex.getId(), edgeType).encode(), edgeValue);
	// Put to Edges Table
	Put edgePut = new Put(IdEncoder.encodeRowId(edgeId));
	edgePut.addColumn(VERICES_COLUMN_FAMILY_BYTES, START_VERTEXID_COLUMN_BYTES,
		IdEncoder.encodeRowId(startVertex.getId()));
	edgePut.addColumn(VERICES_COLUMN_FAMILY_BYTES, TARGET_VERTEXID_COLUMN_BYTES,
		IdEncoder.encodeRowId(targetVertex.getId()));
	edgePut.addColumn(LABELS_COLUMN_FAMILIY_BYTES, Bytes.toBytes(edgeType), new byte[0]);
	Put labelIndexPut = createEdgeLabelIndexPut(edgeId, edgeType);
	List<Put> propertyIndexPuts = new ArrayList<>();
	for (Entry<String, Object> property : properties.entrySet()) {
	    edgePut.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, Bytes.toBytes(property.getKey()),
		    SerializationUtils.serialize((Serializable) property.getValue()));
	    propertyIndexPuts.add(createEdgePropertyIndexPut(edgeId, property.getKey(), property.getValue()));
	}
	// Add to transaction
	put(VERTICES_TABLE, outPut);
	put(VERTICES_TABLE, inPut);
	put(EDGES_TABLE, edgePut);
	put(EDGE_LABELS_INDEX_TABLE, labelIndexPut);
	put(EDGE_PROPERTIES_INDEX_TABLE, propertyIndexPuts);
	DuctileDBEdgeImpl edge = new DuctileDBEdgeImpl(graph, edgeId, edgeType, startVertex, targetVertex,
		new HashMap<>());
	((DuctileDBVertexImpl) targetVertex).addEdge(edge);
	return edge;
    }

    @Override
    public DuctileDBEdge getEdge(long edgeId) {
	try (Table table = openEdgeTable()) {
	    byte[] id = IdEncoder.encodeRowId(edgeId);
	    Get get = new Get(id);
	    Result result = table.get(get);
	    return ResultDecoder.toEdge(graph, edgeId, result);
	} catch (IOException e) {
	    throw new DuctileDBException("Could not get edge.", e);
	}
    }

    @Override
    public Iterable<DuctileDBEdge> getEdges() {
	try (Table table = openEdgeTable()) {
	    ResultScanner result = table.getScanner(new Scan());
	    return new EdgeIterable(graph, result);
	} catch (IOException e) {
	    throw new DuctileDBException("Could not get edge.", e);
	}
    }

    @Override
    public Iterable<DuctileDBEdge> getEdges(String propertyKey, Object propertyValue) {
	try (Table table = openEdgePropertyTable()) {
	    Result result = table.get(new Get(Bytes.toBytes(propertyKey)));
	    NavigableMap<byte[], byte[]> map = result.getFamilyMap(INDEX_COLUMN_FAMILY_BYTES);
	    List<DuctileDBEdge> edges = new ArrayList<>();
	    for (Entry<byte[], byte[]> entry : map.entrySet()) {
		Object value = SerializationUtils.deserialize(entry.getValue());
		if (value.equals(propertyValue)) {
		    edges.add(getEdge(IdEncoder.decodeRowId(entry.getKey())));
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
    public Iterable<DuctileDBEdge> getEdges(String edgeType) {
	try (Table table = openEdgeLabelTable()) {
	    Result result = table.get(new Get(Bytes.toBytes(edgeType)));
	    NavigableMap<byte[], byte[]> map = result.getFamilyMap(INDEX_COLUMN_FAMILY_BYTES);
	    List<DuctileDBEdge> edges = new ArrayList<>();
	    for (byte[] edgeId : map.keySet()) {
		edges.add(getEdge(IdEncoder.decodeRowId(edgeId)));
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
	try (Table vertexTable = openVertexTable()) {
	    byte[] id = IdEncoder.encodeRowId(vertexId);
	    Get get = new Get(id);
	    Result result = vertexTable.get(get);
	    return ResultDecoder.toVertex(graph, vertexId, result);
	} catch (IOException e) {
	    throw new DuctileDBException("Could not get vertex.", e);
	}
    }

    @Override
    public Iterable<DuctileDBVertex> getVertices() {
	try (Table table = openVertexTable()) {
	    ResultScanner result = table.getScanner(new Scan());
	    return new VertexIterable(graph, result);
	} catch (IOException e) {
	    throw new DuctileDBException("Could not get vertices.", e);
	}
    }

    @Override
    public Iterable<DuctileDBVertex> getVertices(String propertyKey, Object propertyValue) {
	try (Table table = openVertexPropertyTable()) {
	    List<DuctileDBVertex> vertices = new ArrayList<>();
	    Get get = new Get(Bytes.toBytes(propertyKey));
	    get.addFamily(INDEX_COLUMN_FAMILY_BYTES);
	    Result result = table.get(get);
	    NavigableMap<byte[], byte[]> propertyMap = result.getFamilyMap(INDEX_COLUMN_FAMILY_BYTES);
	    if (propertyMap != null) {
		for (Entry<byte[], byte[]> entry : propertyMap.entrySet()) {
		    Object value = SerializationUtils.deserialize(entry.getValue());
		    if (propertyValue.equals(value)) {
			long key = IdEncoder.decodeRowId(entry.getKey());
			DuctileDBVertex vertex = getVertex(key);
			if (vertex != null) {
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
    public Iterable<DuctileDBVertex> getVertices(String label) {
	try (Table table = openVertexLabelTable()) {
	    List<DuctileDBVertex> vertices = new ArrayList<>();
	    Get get = new Get(Bytes.toBytes(label));
	    get.addFamily(INDEX_COLUMN_FAMILY_BYTES);
	    Result result = table.get(get);
	    NavigableMap<byte[], byte[]> propertyMap = result.getFamilyMap(INDEX_COLUMN_FAMILY_BYTES);
	    if (propertyMap != null) {
		for (byte[] vertexId : propertyMap.keySet()) {
		    vertices.add(getVertex(IdEncoder.decodeRowId(vertexId)));
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
	Delete deleteOutEdge = new Delete(IdEncoder.encodeRowId(edge.getVertex(EdgeDirection.OUT).getId()));
	long edgeId = edge.getId();
	deleteOutEdge.addColumns(EDGES_COLUMN_FAMILY_BYTES,
		new EdgeKey(EdgeDirection.OUT, edgeId, edge.getVertex(EdgeDirection.IN).getId(), edge.getLabel())
			.encode());
	Delete deleteInEdge = new Delete(IdEncoder.encodeRowId(edge.getVertex(EdgeDirection.IN).getId()));
	deleteInEdge.addColumns(EDGES_COLUMN_FAMILY_BYTES,
		new EdgeKey(EdgeDirection.IN, edgeId, edge.getVertex(EdgeDirection.OUT).getId(), edge.getLabel())
			.encode());
	Delete deleteEdges = new Delete(IdEncoder.encodeRowId(edgeId));
	Delete labelIndex = createEdgeLabelIndexDelete(edgeId, edge.getLabel());
	List<Delete> propertyIndices = new ArrayList<>();
	for (String key : edge.getPropertyKeys()) {
	    propertyIndices.add(createEdgePropertyIndexDelete(edgeId, key));
	}
	// Add to transaction
	delete(VERTICES_TABLE, deleteOutEdge);
	delete(VERTICES_TABLE, deleteInEdge);
	delete(EDGES_TABLE, deleteEdges);
	delete(EDGE_LABELS_INDEX_TABLE, labelIndex);
	delete(EDGE_PROPERTIES_INDEX_TABLE, propertyIndices);
    }

    @Override
    public void removeVertex(DuctileDBVertex vertex) {
	long vertexId = vertex.getId();
	byte[] id = IdEncoder.encodeRowId(vertexId);
	for (DuctileDBEdge edge : vertex.getEdges(EdgeDirection.BOTH)) {
	    removeEdge(edge);
	}
	for (String label : vertex.getLabels()) {
	    Delete delete = createVertexLabelIndexDelete(vertexId, label);
	    delete(VERTEX_LABELS_INDEX_TABLE, delete);
	}
	for (String key : vertex.getPropertyKeys()) {
	    Delete delete = createVertexPropertyIndexDelete(vertexId, key);
	    delete(VERTEX_PROPERTIES_INDEX_TABLE, delete);
	}
	Delete delete = new Delete(id);
	delete(VERTICES_TABLE, delete);
    }

    @Override
    public void addLabel(DuctileDBVertex vertex, String label) {
	byte[] id = IdEncoder.encodeRowId(vertex.getId());
	Put put = new Put(id);
	put.addColumn(LABELS_COLUMN_FAMILIY_BYTES, Bytes.toBytes(label), Bytes.toBytes(label));
	Put index = createVertexLabelIndexPut(vertex.getId(), label);
	// Add to transaction
	put(VERTICES_TABLE, put);
	put(VERTEX_LABELS_INDEX_TABLE, index);
    }

    @Override
    public void removeLabel(DuctileDBVertex vertex, String label) {
	byte[] id = IdEncoder.encodeRowId(vertex.getId());
	Delete delete = new Delete(id);
	delete.addColumn(LABELS_COLUMN_FAMILIY_BYTES, Bytes.toBytes(label));
	Delete index = createVertexLabelIndexDelete(vertex.getId(), label);
	// Add to transaction
	delete(VERTICES_TABLE, delete);
	delete(VERTEX_LABELS_INDEX_TABLE, index);
    }

    @Override
    public void setProperty(DuctileDBVertex vertex, String key, Object value) {
	byte[] id = IdEncoder.encodeRowId(vertex.getId());
	Put put = new Put(id);
	put.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, Bytes.toBytes(key),
		SerializationUtils.serialize((Serializable) value));
	Put index = createVertexPropertyIndexPut(vertex.getId(), key, value);
	// Add to transaction
	put(VERTICES_TABLE, put);
	put(VERTEX_PROPERTIES_INDEX_TABLE, index);
    }

    @Override
    public void removeProperty(DuctileDBVertex vertex, String key) {
	byte[] id = IdEncoder.encodeRowId(vertex.getId());
	Delete delete = new Delete(id);
	delete.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, Bytes.toBytes(key));
	Delete index = createVertexPropertyIndexDelete(vertex.getId(), key);
	// Add to transaction
	delete(VERTICES_TABLE, delete);
	delete(VERTEX_PROPERTIES_INDEX_TABLE, index);
    }

    @Override
    public void setProperty(DuctileDBEdge edge, String key, Object value) {
	try (Table table = openVertexTable()) {
	    long startVertexId = edge.getStartVertex().getId();
	    long targetVertexId = edge.getTargetVertex().getId();
	    byte[] startVertexRowId = IdEncoder.encodeRowId(startVertexId);
	    byte[] targetVertexRowId = IdEncoder.encodeRowId(targetVertexId);
	    EdgeKey startVertexEdgeKey = new EdgeKey(EdgeDirection.OUT, edge.getId(), targetVertexId, edge.getLabel());
	    EdgeKey targetVertexEdgeKey = new EdgeKey(EdgeDirection.IN, edge.getId(), startVertexId, edge.getLabel());
	    Result startVertexResult = table.get(new Get(startVertexRowId));
	    byte[] startVertexPropertyBytes = startVertexResult
		    .getColumnLatestCell(EDGES_COLUMN_FAMILY_BYTES, startVertexEdgeKey.encode()).getValueArray();
	    @SuppressWarnings("unchecked")
	    Map<String, Object> startVertexProperties = (Map<String, Object>) SerializationUtils
		    .deserialize(startVertexPropertyBytes);
	    startVertexProperties.put(key, value);
	    Put startVertexPut = new Put(startVertexRowId);
	    startVertexPut.addColumn(EDGES_COLUMN_FAMILY_BYTES, startVertexEdgeKey.encode(),
		    SerializationUtils.serialize((Serializable) startVertexProperties));
	    Result targetVertexResult = table.get(new Get(targetVertexRowId));
	    byte[] targetVertexPropertyBytes = targetVertexResult
		    .getColumnLatestCell(EDGES_COLUMN_FAMILY_BYTES, targetVertexEdgeKey.encode()).getValueArray();
	    @SuppressWarnings("unchecked")
	    Map<String, Object> targetVertexProperties = (Map<String, Object>) SerializationUtils
		    .deserialize(targetVertexPropertyBytes);
	    targetVertexProperties.put(key, value);
	    Put targetVertexPut = new Put(targetVertexRowId);
	    targetVertexPut.addColumn(EDGES_COLUMN_FAMILY_BYTES, targetVertexEdgeKey.encode(),
		    SerializationUtils.serialize((Serializable) targetVertexProperties));
	    Put index = createEdgePropertyIndexPut(edge.getId(), key, value);
	    // Add to transaction
	    put(VERTICES_TABLE, startVertexPut);
	    put(VERTICES_TABLE, targetVertexPut);
	    put(EDGE_PROPERTIES_INDEX_TABLE, index);
	} catch (IOException e) {
	    throw new DuctileDBException("Could not set edge property (id='" + edge.getId().toString() + "').", e);
	}
    }

    @Override
    public void removeProperty(DuctileDBEdge edge, String key) {
	try (Table table = openVertexTable()) {
	    long startVertexId = edge.getStartVertex().getId();
	    long targetVertexId = edge.getTargetVertex().getId();
	    byte[] startVertexRowId = IdEncoder.encodeRowId(startVertexId);
	    byte[] targetVertexRowId = IdEncoder.encodeRowId(targetVertexId);
	    EdgeKey startVertexEdgeKey = new EdgeKey(EdgeDirection.OUT, edge.getId(), targetVertexId, edge.getLabel());
	    EdgeKey targetVertexEdgeKey = new EdgeKey(EdgeDirection.IN, edge.getId(), startVertexId, edge.getLabel());
	    Result startVertexResult = table.get(new Get(startVertexRowId));
	    byte[] startVertexPropertyBytes = startVertexResult
		    .getColumnLatestCell(EDGES_COLUMN_FAMILY_BYTES, startVertexEdgeKey.encode()).getValueArray();
	    @SuppressWarnings("unchecked")
	    Map<String, Object> startVertexProperties = (Map<String, Object>) SerializationUtils
		    .deserialize(startVertexPropertyBytes);
	    startVertexProperties.remove(key);
	    Put startVertexPut = new Put(startVertexRowId);
	    startVertexPut.addColumn(EDGES_COLUMN_FAMILY_BYTES, startVertexEdgeKey.encode(),
		    SerializationUtils.serialize((Serializable) startVertexProperties));
	    Result targetVertexResult = table.get(new Get(targetVertexRowId));
	    byte[] targetVertexPropertyBytes = targetVertexResult
		    .getColumnLatestCell(EDGES_COLUMN_FAMILY_BYTES, targetVertexEdgeKey.encode()).getValueArray();
	    @SuppressWarnings("unchecked")
	    Map<String, Object> targetVertexProperties = (Map<String, Object>) SerializationUtils
		    .deserialize(targetVertexPropertyBytes);
	    targetVertexProperties.remove(key);
	    Put targetVertexPut = new Put(targetVertexRowId);
	    targetVertexPut.addColumn(EDGES_COLUMN_FAMILY_BYTES, targetVertexEdgeKey.encode(),
		    SerializationUtils.serialize((Serializable) targetVertexProperties));
	    Delete index = createEdgePropertyIndexDelete(edge.getId(), key);
	    // Add to transaction
	    put(VERTICES_TABLE, startVertexPut);
	    put(VERTICES_TABLE, targetVertexPut);
	    delete(EDGE_PROPERTIES_INDEX_TABLE, index);
	} catch (IOException e) {
	    throw new DuctileDBException("Could not set edge property (id='" + edge.getId().toString() + "').", e);
	}
    }

}
