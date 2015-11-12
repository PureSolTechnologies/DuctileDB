package com.puresoltechnologies.ductiledb;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.tx.DuctileDBTransaction;
import com.puresoltechnologies.ductiledb.utils.IdEncoder;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;

public class DuctileDBGraphImpl implements DuctileDBGraph {

    static final long ID_CACHE = 100;

    static final String DUCTILEDB_NAMESPACE = "ductiledb";
    static final String METADATA_TABLE = DUCTILEDB_NAMESPACE + ":metadata";
    static final String VERTICES_TABLE = DUCTILEDB_NAMESPACE + ":vertices";
    static final String EDGES_TABLE = DUCTILEDB_NAMESPACE + ":edges";
    static final String VERTEX_PROPERTIES_INDEX_TABLE = DUCTILEDB_NAMESPACE + ":vertex_properties";
    static final String VERTEX_LABELS_INDEX_TABLE = DUCTILEDB_NAMESPACE + ":vertex_labels";
    static final String EDGE_PROPERTIES_INDEX_TABLE = DUCTILEDB_NAMESPACE + ":edge_properties";
    static final String EDGE_LABELS_INDEX_TABLE = DUCTILEDB_NAMESPACE + ":edge_labels";

    static final String METADATA_COLUMN_FAMILIY = "metadata";
    static final byte[] METADATA_COLUMN_FAMILIY_BYTES = Bytes.toBytes(METADATA_COLUMN_FAMILIY);

    static final String ID_ROW = "IdRow";
    static final byte[] ID_ROW_BYTES = Bytes.toBytes(ID_ROW);

    static final String VERTEXID_COLUMN = "VertexId";
    static final byte[] VERTEXID_COLUMN_BYTES = Bytes.toBytes(VERTEXID_COLUMN);

    static final String START_VERTEXID_COLUMN = "StartVertexId";
    static final byte[] START_VERTEXID_COLUMN_BYTES = Bytes.toBytes(START_VERTEXID_COLUMN);

    static final String TARGET_VERTEXID_COLUMN = "TargetVertexId";
    static final byte[] TARGET_VERTEXID_COLUMN_BYTES = Bytes.toBytes(TARGET_VERTEXID_COLUMN);

    static final String EDGEID_COLUMN = "EdgeId";
    static final byte[] EDGEID_COLUMN_BYTES = Bytes.toBytes(EDGEID_COLUMN);

    static final String LABELS_COLUMN_FAMILIY = "labels";
    static final byte[] LABELS_COLUMN_FAMILIY_BYTES = Bytes.toBytes(LABELS_COLUMN_FAMILIY);

    static final String EDGES_COLUMN_FAMILY = "edges";
    static final byte[] EDGES_COLUMN_FAMILY_BYTES = Bytes.toBytes(EDGES_COLUMN_FAMILY);

    static final String PROPERTIES_COLUMN_FAMILY = "properties";
    static final byte[] PROPERTIES_COLUMN_FAMILY_BYTES = Bytes.toBytes(PROPERTIES_COLUMN_FAMILY);

    static final String VERICES_COLUMN_FAMILY = "vertices";
    static final byte[] VERICES_COLUMN_FAMILY_BYTES = Bytes.toBytes(VERICES_COLUMN_FAMILY);

    static final String INDEX_COLUMN_FAMILY = "index";
    static final byte[] INDEX_COLUMN_FAMILY_BYTES = Bytes.toBytes(INDEX_COLUMN_FAMILY);

    static final String DUCTILEDB_ID_PROPERTY = "_ductiledb.id";

    private final ThreadLocal<DuctileDBTransaction> transactions = new ThreadLocal<DuctileDBTransaction>() {
	@Override
	protected DuctileDBTransaction initialValue() {
	    return new DuctileDBTransaction();
	}
    };

    private final Connection connection;
    private long vertexIdCounter = ID_CACHE;
    private long edgeIdCounter = ID_CACHE;
    private long nextVertexId = -1;
    private long nextEdgeId = -1;

    public DuctileDBGraphImpl(Connection connection) throws IOException {
	this.connection = connection;
	checkAndCreateEnvironment();
    }

    private void checkAndCreateEnvironment() throws IOException {
	try (Admin admin = connection.getAdmin()) {
	    assureNamespacePresence(admin);
	    assureMetaDataTablePresence(admin);
	    assureVerticesTablePresence(admin);
	    assureEdgesTablePresence(admin);
	    assureVertexLabelsIndexTablePresence(admin);
	    assureVertexPropertiesIndexTablePresence(admin);
	    assureEdgeLabelsIndexTablePresence(admin);
	    assureEdgePropertiesIndexTablePresence(admin);
	}
    }

    private void assureNamespacePresence(Admin admin) throws IOException {
	boolean foundNamespace = false;
	for (NamespaceDescriptor namespaceDescriptor : admin.listNamespaceDescriptors()) {
	    if (DUCTILEDB_NAMESPACE.equals(namespaceDescriptor.getName())) {
		foundNamespace = true;
	    }
	}
	if (!foundNamespace) {
	    NamespaceDescriptor descriptor = NamespaceDescriptor.create(DUCTILEDB_NAMESPACE).build();
	    admin.createNamespace(descriptor);
	}
    }

    private void assureMetaDataTablePresence(Admin admin) throws IOException {
	if (!admin.isTableAvailable(TableName.valueOf(METADATA_TABLE))) {
	    HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(METADATA_TABLE));
	    HColumnDescriptor metaDataColumnFamily = new HColumnDescriptor(METADATA_COLUMN_FAMILIY);
	    descriptor.addFamily(metaDataColumnFamily);
	    admin.createTable(descriptor);
	    try (Table table = openMetaDataTable()) {
		Put vertexIdPut = new Put(VERTEXID_COLUMN_BYTES);
		vertexIdPut.addColumn(METADATA_COLUMN_FAMILIY_BYTES, VERTEXID_COLUMN_BYTES, Bytes.toBytes(0l));
		Put edgeIdPut = new Put(EDGEID_COLUMN_BYTES);
		edgeIdPut.addColumn(METADATA_COLUMN_FAMILIY_BYTES, EDGEID_COLUMN_BYTES, Bytes.toBytes(0l));
		table.put(Arrays.asList(vertexIdPut, edgeIdPut));
	    }
	}
    }

    private void assureVerticesTablePresence(Admin admin) throws IOException {
	if (!admin.isTableAvailable(TableName.valueOf(VERTICES_TABLE))) {
	    HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(VERTICES_TABLE));
	    HColumnDescriptor labelColumnFamily = new HColumnDescriptor(LABELS_COLUMN_FAMILIY);
	    descriptor.addFamily(labelColumnFamily);
	    HColumnDescriptor edgesColumnFamily = new HColumnDescriptor(EDGES_COLUMN_FAMILY);
	    descriptor.addFamily(edgesColumnFamily);
	    HColumnDescriptor propertiesColumnFamily = new HColumnDescriptor(PROPERTIES_COLUMN_FAMILY);
	    descriptor.addFamily(propertiesColumnFamily);
	    admin.createTable(descriptor);
	}
    }

    private void assureEdgesTablePresence(Admin admin) throws IOException {
	if (!admin.isTableAvailable(TableName.valueOf(EDGES_TABLE))) {
	    HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(EDGES_TABLE));
	    HColumnDescriptor labelsColumnFamily = new HColumnDescriptor(LABELS_COLUMN_FAMILIY_BYTES);
	    descriptor.addFamily(labelsColumnFamily);
	    HColumnDescriptor propertiesColumnFamily = new HColumnDescriptor(PROPERTIES_COLUMN_FAMILY_BYTES);
	    descriptor.addFamily(propertiesColumnFamily);
	    HColumnDescriptor verticesColumnFamily = new HColumnDescriptor(VERICES_COLUMN_FAMILY_BYTES);
	    descriptor.addFamily(verticesColumnFamily);
	    admin.createTable(descriptor);
	}
    }

    private void assureVertexLabelsIndexTablePresence(Admin admin) throws IOException {
	if (!admin.isTableAvailable(TableName.valueOf(VERTEX_LABELS_INDEX_TABLE))) {
	    HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(VERTEX_LABELS_INDEX_TABLE));
	    HColumnDescriptor indexColumnFamily = new HColumnDescriptor(INDEX_COLUMN_FAMILY_BYTES);
	    descriptor.addFamily(indexColumnFamily);
	    admin.createTable(descriptor);
	}
    }

    private void assureVertexPropertiesIndexTablePresence(Admin admin) throws IOException {
	if (!admin.isTableAvailable(TableName.valueOf(VERTEX_PROPERTIES_INDEX_TABLE))) {
	    HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(VERTEX_PROPERTIES_INDEX_TABLE));
	    HColumnDescriptor indexColumnFamily = new HColumnDescriptor(INDEX_COLUMN_FAMILY_BYTES);
	    descriptor.addFamily(indexColumnFamily);
	    admin.createTable(descriptor);
	}
    }

    private void assureEdgeLabelsIndexTablePresence(Admin admin) throws IOException {
	if (!admin.isTableAvailable(TableName.valueOf(EDGE_LABELS_INDEX_TABLE))) {
	    HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(EDGE_LABELS_INDEX_TABLE));
	    HColumnDescriptor indexColumnFamily = new HColumnDescriptor(INDEX_COLUMN_FAMILY_BYTES);
	    descriptor.addFamily(indexColumnFamily);
	    admin.createTable(descriptor);
	}
    }

    private void assureEdgePropertiesIndexTablePresence(Admin admin) throws IOException {
	if (!admin.isTableAvailable(TableName.valueOf(EDGE_PROPERTIES_INDEX_TABLE))) {
	    HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(EDGE_PROPERTIES_INDEX_TABLE));
	    HColumnDescriptor indexColumnFamily = new HColumnDescriptor(INDEX_COLUMN_FAMILY_BYTES);
	    descriptor.addFamily(indexColumnFamily);
	    admin.createTable(descriptor);
	}
    }

    @Override
    public void close() throws IOException {
	connection.close();
    }

    @Override
    public Connection getConnection() {
	return connection;
    }

    private DuctileDBTransaction getCurrentTransaction() {
	return transactions.get();
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
    public DuctileDBEdge addEdge(Vertex startVertex, Vertex targetVertex, String edgeType) {
	return addEdge(startVertex, targetVertex, edgeType, new HashMap<>());
    }

    @Override
    public DuctileDBEdge addEdge(Object edgeId, Vertex startVertex, Vertex targetVertex, String edgeType) {
	return addEdge(startVertex, targetVertex, edgeType, new HashMap<>());
    }

    @Override
    public DuctileDBEdge addEdge(Vertex startVertex, Vertex targetVertex, String edgeType,
	    Map<String, Object> properties) {
	long edgeId = createEdgeId();
	byte[] edgeValue = new EdgeValue(properties).encode();
	// Put to Start Vertex
	Put outPut = new Put(IdEncoder.encodeRowId((long) startVertex.getId()));
	outPut.addColumn(EDGES_COLUMN_FAMILY_BYTES,
		new EdgeKey(Direction.OUT, edgeId, (long) targetVertex.getId(), edgeType).encode(), edgeValue);
	// Put to Target Vertex
	Put inPut = new Put(IdEncoder.encodeRowId((long) targetVertex.getId()));
	inPut.addColumn(EDGES_COLUMN_FAMILY_BYTES,
		new EdgeKey(Direction.IN, edgeId, (long) startVertex.getId(), edgeType).encode(), edgeValue);
	// Put to Edges Table
	Put edgePut = new Put(IdEncoder.encodeRowId(edgeId));
	edgePut.addColumn(VERICES_COLUMN_FAMILY_BYTES, START_VERTEXID_COLUMN_BYTES,
		IdEncoder.encodeRowId((long) startVertex.getId()));
	edgePut.addColumn(VERICES_COLUMN_FAMILY_BYTES, TARGET_VERTEXID_COLUMN_BYTES,
		IdEncoder.encodeRowId((long) targetVertex.getId()));
	edgePut.addColumn(LABELS_COLUMN_FAMILIY_BYTES, Bytes.toBytes(edgeType), new byte[0]);
	Put labelIndexPut = createEdgeLabelIndexPut(edgeId, edgeType);
	List<Put> propertyIndexPuts = new ArrayList<>();
	for (Entry<String, Object> property : properties.entrySet()) {
	    edgePut.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, Bytes.toBytes(property.getKey()),
		    SerializationUtils.serialize((Serializable) property.getValue()));
	    propertyIndexPuts.add(createEdgePropertyIndexPut(edgeId, property.getKey(), property.getValue()));
	}
	// Add to transaction
	DuctileDBTransaction currentTransaction = getCurrentTransaction();
	currentTransaction.put(VERTICES_TABLE, outPut);
	currentTransaction.put(VERTICES_TABLE, inPut);
	currentTransaction.put(EDGES_TABLE, edgePut);
	currentTransaction.put(EDGE_LABELS_INDEX_TABLE, labelIndexPut);
	currentTransaction.put(EDGE_PROPERTIES_INDEX_TABLE, propertyIndexPuts);
	DuctileDBEdgeImpl edge = new DuctileDBEdgeImpl(this, edgeId, edgeType, (DuctileDBVertex) startVertex,
		(DuctileDBVertex) targetVertex, new HashMap<>());
	((DuctileDBVertexImpl) targetVertex).addEdge(edge);
	return edge;
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
    public DuctileDBVertex addVertex(Object vertexId) {
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
	// Add to transaction
	DuctileDBTransaction currentTransaction = getCurrentTransaction();
	currentTransaction.put(VERTICES_TABLE, put);
	currentTransaction.put(VERTEX_LABELS_INDEX_TABLE, labelIndex);
	currentTransaction.put(VERTEX_PROPERTIES_INDEX_TABLE, propertyIndex);
	return new DuctileDBVertexImpl(this, vertexId, labels, properties, new ArrayList<>());
    }

    @Override
    public DuctileDBEdge getEdge(long edgeId) {
	try (Table table = openEdgeTable()) {
	    byte[] id = IdEncoder.encodeRowId(edgeId);
	    Get get = new Get(id);
	    Result result = table.get(get);
	    return ResultDecoder.toEdge(this, edgeId, result);
	} catch (IOException e) {
	    throw new DuctileDBException("Could not get edge.", e);
	}
    }

    @Override
    public DuctileDBEdge getEdge(Object edgeId) {
	return getEdge((long) edgeId);
    }

    @Override
    public Iterable<Edge> getEdges() {
	try (Table table = openEdgeTable()) {
	    ResultScanner result = table.getScanner(new Scan());
	    return new EdgeIterable(this, result);
	} catch (IOException e) {
	    throw new DuctileDBException("Could not get edge.", e);
	}
    }

    @Override
    public Iterable<Edge> getEdges(String propertyKey, Object propertyValue) {
	try (Table table = openEdgePropertyTable()) {
	    Result result = table.get(new Get(Bytes.toBytes(propertyKey)));
	    NavigableMap<byte[], byte[]> map = result.getFamilyMap(INDEX_COLUMN_FAMILY_BYTES);
	    List<Edge> edges = new ArrayList<>();
	    for (Entry<byte[], byte[]> entry : map.entrySet()) {
		Object value = SerializationUtils.deserialize(entry.getValue());
		if (value.equals(propertyValue)) {
		    edges.add(getEdge(IdEncoder.decodeRowId(entry.getKey())));
		}
	    }
	    return new Iterable<Edge>() {
		@Override
		public Iterator<Edge> iterator() {
		    return edges.iterator();
		}
	    };
	} catch (IOException e) {
	    throw new DuctileDBException("Could not get edges.", e);
	}
    }

    @Override
    public Iterable<Edge> getEdges(String edgeType) {
	try (Table table = openEdgeLabelTable()) {
	    Result result = table.get(new Get(Bytes.toBytes(edgeType)));
	    NavigableMap<byte[], byte[]> map = result.getFamilyMap(INDEX_COLUMN_FAMILY_BYTES);
	    List<Edge> edges = new ArrayList<>();
	    for (byte[] edgeId : map.keySet()) {
		edges.add(getEdge(IdEncoder.decodeRowId(edgeId)));
	    }
	    return new Iterable<Edge>() {
		@Override
		public Iterator<Edge> iterator() {
		    return edges.iterator();
		}
	    };
	} catch (IOException e) {
	    throw new DuctileDBException("Could not get edges.", e);
	}
    }

    @Override
    public Features getFeatures() {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public DuctileDBVertex getVertex(long vertexId) {
	try (Table vertexTable = openVertexTable()) {
	    byte[] id = IdEncoder.encodeRowId(vertexId);
	    Get get = new Get(id);
	    Result result = vertexTable.get(get);
	    return ResultDecoder.toVertex(this, vertexId, result);
	} catch (IOException e) {
	    throw new DuctileDBException("Could not get vertex.", e);
	}
    }

    @Override
    public DuctileDBVertex getVertex(Object vertexId) {
	return getVertex((long) vertexId);
    }

    @Override
    public Iterable<Vertex> getVertices() {
	try (Table table = openVertexTable()) {
	    ResultScanner result = table.getScanner(new Scan());
	    return new VertexIterable(this, result);
	} catch (IOException e) {
	    throw new DuctileDBException("Could not get vertices.", e);
	}
    }

    @Override
    public Iterable<Vertex> getVertices(String propertyKey, Object propertyValue) {
	try (Table table = openVertexPropertyTable()) {
	    List<Vertex> vertices = new ArrayList<>();
	    Get get = new Get(Bytes.toBytes(propertyKey));
	    get.addFamily(INDEX_COLUMN_FAMILY_BYTES);
	    Result result = table.get(get);
	    NavigableMap<byte[], byte[]> propertyMap = result.getFamilyMap(INDEX_COLUMN_FAMILY_BYTES);
	    if (propertyMap != null) {
		for (Entry<byte[], byte[]> entry : propertyMap.entrySet()) {
		    Object value = SerializationUtils.deserialize(entry.getValue());
		    if (propertyValue.equals(value)) {
			Object key = IdEncoder.decodeRowId(entry.getKey());
			DuctileDBVertex vertex = getVertex(key);
			if (vertex != null) {
			    vertices.add(vertex);
			}
		    }
		}
	    }
	    return new Iterable<Vertex>() {
		@Override
		public Iterator<Vertex> iterator() {
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
    public GraphQuery query() {
	// TODO to be implemented...
	return null;
    }

    @Override
    public void removeEdge(Edge edge) {
	Delete deleteOutEdge = new Delete(IdEncoder.encodeRowId((long) edge.getVertex(Direction.OUT).getId()));
	long edgeId = (long) edge.getId();
	deleteOutEdge.addColumns(EDGES_COLUMN_FAMILY_BYTES,
		new EdgeKey(Direction.OUT, edgeId, (long) edge.getVertex(Direction.IN).getId(), edge.getLabel())
			.encode());
	Delete deleteInEdge = new Delete(IdEncoder.encodeRowId((long) edge.getVertex(Direction.IN).getId()));
	deleteInEdge.addColumns(EDGES_COLUMN_FAMILY_BYTES,
		new EdgeKey(Direction.IN, edgeId, (long) edge.getVertex(Direction.OUT).getId(), edge.getLabel())
			.encode());
	Delete deleteEdges = new Delete(IdEncoder.encodeRowId(edgeId));
	Delete labelIndex = createEdgeLabelIndexDelete(edgeId, edge.getLabel());
	List<Delete> propertyIndices = new ArrayList<>();
	for (String key : edge.getPropertyKeys()) {
	    propertyIndices.add(createEdgePropertyIndexDelete(edgeId, key));
	}
	// Add to transaction
	DuctileDBTransaction currentTransaction = getCurrentTransaction();
	currentTransaction.delete(VERTICES_TABLE, deleteOutEdge);
	currentTransaction.delete(VERTICES_TABLE, deleteInEdge);
	currentTransaction.delete(EDGES_TABLE, deleteEdges);
	currentTransaction.delete(EDGE_LABELS_INDEX_TABLE, labelIndex);
	currentTransaction.delete(EDGE_PROPERTIES_INDEX_TABLE, propertyIndices);
    }

    @Override
    public void removeVertex(Vertex vertex) {
	long vertexId = (long) vertex.getId();
	byte[] id = IdEncoder.encodeRowId(vertexId);
	for (Edge edge : ((DuctileDBVertex) vertex).getEdges(Direction.BOTH)) {
	    removeEdge(edge);
	}
	for (String label : ((DuctileDBVertex) vertex).getLabels()) {
	    Delete delete = createVertexLabelIndexDelete(vertexId, label);
	    getCurrentTransaction().delete(VERTEX_LABELS_INDEX_TABLE, delete);
	}
	for (String key : vertex.getPropertyKeys()) {
	    Delete delete = createVertexPropertyIndexDelete(vertexId, key);
	    getCurrentTransaction().delete(VERTEX_PROPERTIES_INDEX_TABLE, delete);
	}
	Delete delete = new Delete(id);
	getCurrentTransaction().delete(VERTICES_TABLE, delete);
    }

    @Override
    public void shutdown() {
	try {
	    close();
	} catch (IOException e) {
	    throw new DuctileDBException("Could not shutdown graph.", e);
	}
    }

    @Override
    @SuppressWarnings("deprecation")
    public void stopTransaction(Conclusion conclusion) {
	switch (conclusion) {
	case SUCCESS:
	    commit();
	    break;
	case FAILURE:
	    rollback();
	    break;
	default:
	    rollback();
	}
    }

    @Override
    public void commit() {
	getCurrentTransaction().commit(connection);
    }

    @Override
    public void rollback() {
	getCurrentTransaction().rollback();
    }

    public void addLabel(DuctileDBVertex vertex, String label) {
	byte[] id = IdEncoder.encodeRowId(vertex.getId());
	Put put = new Put(id);
	put.addColumn(LABELS_COLUMN_FAMILIY_BYTES, Bytes.toBytes(label), Bytes.toBytes(label));
	Put index = createVertexLabelIndexPut(vertex.getId(), label);
	// Add to transaction
	DuctileDBTransaction currentTransaction = getCurrentTransaction();
	currentTransaction.put(VERTICES_TABLE, put);
	currentTransaction.put(VERTEX_LABELS_INDEX_TABLE, index);
    }

    public void removeLabel(DuctileDBVertex vertex, String label) {
	byte[] id = IdEncoder.encodeRowId(vertex.getId());
	Delete delete = new Delete(id);
	delete.addColumn(LABELS_COLUMN_FAMILIY_BYTES, Bytes.toBytes(label));
	Delete index = createVertexLabelIndexDelete(vertex.getId(), label);
	// Add to transaction
	DuctileDBTransaction currentTransaction = getCurrentTransaction();
	currentTransaction.delete(VERTICES_TABLE, delete);
	currentTransaction.delete(VERTEX_LABELS_INDEX_TABLE, index);
    }

    void setVertexProperty(DuctileDBVertex vertex, String key, Object value) {
	byte[] id = IdEncoder.encodeRowId(vertex.getId());
	Put put = new Put(id);
	put.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, Bytes.toBytes(key),
		SerializationUtils.serialize((Serializable) value));
	Put index = createVertexPropertyIndexPut(vertex.getId(), key, value);
	// Add to transaction
	DuctileDBTransaction currentTransaction = getCurrentTransaction();
	currentTransaction.put(VERTICES_TABLE, put);
	currentTransaction.put(VERTEX_PROPERTIES_INDEX_TABLE, index);
    }

    public void removeVertexProperty(DuctileDBVertex vertex, String key) {
	byte[] id = IdEncoder.encodeRowId(vertex.getId());
	Delete delete = new Delete(id);
	delete.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, Bytes.toBytes(key));
	Delete index = createVertexPropertyIndexDelete(vertex.getId(), key);
	// Add to transaction
	DuctileDBTransaction currentTransaction = getCurrentTransaction();
	currentTransaction.delete(VERTICES_TABLE, delete);
	currentTransaction.delete(VERTEX_PROPERTIES_INDEX_TABLE, index);
    }

    public void setEdgeProperty(DuctileDBEdge edge, String key, Object value) {
	try (Table table = openVertexTable()) {
	    long startVertexId = edge.getStartVertex().getId();
	    long targetVertexId = edge.getTargetVertex().getId();
	    byte[] startVertexRowId = IdEncoder.encodeRowId(startVertexId);
	    byte[] targetVertexRowId = IdEncoder.encodeRowId(targetVertexId);
	    EdgeKey startVertexEdgeKey = new EdgeKey(Direction.OUT, edge.getId(), targetVertexId, edge.getLabel());
	    EdgeKey targetVertexEdgeKey = new EdgeKey(Direction.IN, edge.getId(), startVertexId, edge.getLabel());
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
	    DuctileDBTransaction currentTransaction = getCurrentTransaction();
	    currentTransaction.put(VERTICES_TABLE, startVertexPut);
	    currentTransaction.put(VERTICES_TABLE, targetVertexPut);
	    currentTransaction.put(EDGE_PROPERTIES_INDEX_TABLE, index);
	} catch (IOException e) {
	    throw new DuctileDBException("Could not set edge property (id='" + edge.getId().toString() + "').", e);
	}
    }

    public void removeEdgeProperty(DuctileDBEdge edge, String key) {
	try (Table table = openVertexTable()) {
	    long startVertexId = edge.getStartVertex().getId();
	    long targetVertexId = edge.getTargetVertex().getId();
	    byte[] startVertexRowId = IdEncoder.encodeRowId(startVertexId);
	    byte[] targetVertexRowId = IdEncoder.encodeRowId(targetVertexId);
	    EdgeKey startVertexEdgeKey = new EdgeKey(Direction.OUT, edge.getId(), targetVertexId, edge.getLabel());
	    EdgeKey targetVertexEdgeKey = new EdgeKey(Direction.IN, edge.getId(), startVertexId, edge.getLabel());
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
	    DuctileDBTransaction currentTransaction = getCurrentTransaction();
	    currentTransaction.put(VERTICES_TABLE, startVertexPut);
	    currentTransaction.put(VERTICES_TABLE, targetVertexPut);
	    currentTransaction.delete(EDGE_PROPERTIES_INDEX_TABLE, index);
	} catch (IOException e) {
	    throw new DuctileDBException("Could not set edge property (id='" + edge.getId().toString() + "').", e);
	}
    }
}
