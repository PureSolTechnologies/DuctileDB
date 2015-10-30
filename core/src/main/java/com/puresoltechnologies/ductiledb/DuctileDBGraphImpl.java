package com.puresoltechnologies.ductiledb;

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
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.puresoltechnologies.ductiledb.tx.DuctileDBTransaction;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;

public class DuctileDBGraphImpl implements DuctileDBGraph {

    static final long ID_CACHE = 100;

    static final String NAMESPACE_NAME = "ductiledb";
    static final String METADATA_TABLE_NAME = NAMESPACE_NAME + ":metadata";
    static final String VERTICES_TABLE_NAME = NAMESPACE_NAME + ":vertices";
    static final String PROPERTIES_TABLE_NAME = NAMESPACE_NAME + ":properties";
    static final String LABELS_TABLE_NAME = NAMESPACE_NAME + ":labels";

    static final String METADATA_COLUMN_FAMILIY = "metadata";
    static final byte[] METADATA_COLUMN_FAMILIY_BYTES = Bytes.toBytes(METADATA_COLUMN_FAMILIY);

    static final String ID_ROW = "IdRow";
    static final byte[] ID_ROW_BYTES = Bytes.toBytes(ID_ROW);

    static final String VERTEXID_COLUMN = "VertexId";
    static final byte[] VERTEXID_COLUMN_BYTES = Bytes.toBytes(VERTEXID_COLUMN);

    static final String EDGEID_COLUMN = "EdgeId";
    static final byte[] EDGEID_COLUMN_BYTES = Bytes.toBytes(EDGEID_COLUMN);

    static final String LABELS_COLUMN_FAMILIY = "labels";
    static final byte[] LABELS_COLUMN_FAMILIY_BYTES = Bytes.toBytes(LABELS_COLUMN_FAMILIY);

    static final String EDGES_COLUMN_FAMILY = "edges";
    static final byte[] EDGES_COLUMN_FAMILY_BYTES = Bytes.toBytes(EDGES_COLUMN_FAMILY);

    static final String PROPERTIES_COLUMN_FAMILY = "properties";
    static final byte[] PROPERTIES_COLUMN_FAMILY_BYTES = Bytes.toBytes(PROPERTIES_COLUMN_FAMILY);

    static final String INDEX_COLUMN_FAMILY = "index";
    static final byte[] INDEX_COLUMN_FAMILY_BYTES = Bytes.toBytes(INDEX_COLUMN_FAMILY);

    static final String DUCTILEDB_ID_PROPERTY = "_ductiledb.id";
    static final byte[] DUCTILEDB_ID_PROPERTY_BYTES = Bytes.toBytes(DUCTILEDB_ID_PROPERTY);

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
	    assureLabelsTablePresence(admin);
	    assurePropertiesTablePresence(admin);
	}
    }

    private void assureNamespacePresence(Admin admin) throws IOException {
	boolean foundNamespace = false;
	for (NamespaceDescriptor namespaceDescriptor : admin.listNamespaceDescriptors()) {
	    if (NAMESPACE_NAME.equals(namespaceDescriptor.getName())) {
		foundNamespace = true;
	    }
	}
	if (!foundNamespace) {
	    NamespaceDescriptor descriptor = NamespaceDescriptor.create(NAMESPACE_NAME).build();
	    admin.createNamespace(descriptor);
	}
    }

    private void assureMetaDataTablePresence(Admin admin) throws IOException {
	if (!admin.isTableAvailable(TableName.valueOf(METADATA_TABLE_NAME))) {
	    HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(METADATA_TABLE_NAME));
	    HColumnDescriptor metaDataColumnFamily = new HColumnDescriptor(METADATA_COLUMN_FAMILIY);
	    descriptor.addFamily(metaDataColumnFamily);
	    admin.createTable(descriptor);
	}
    }

    private void assureVerticesTablePresence(Admin admin) throws IOException {
	if (!admin.isTableAvailable(TableName.valueOf(VERTICES_TABLE_NAME))) {
	    HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(VERTICES_TABLE_NAME));
	    HColumnDescriptor labelColumnFamily = new HColumnDescriptor(LABELS_COLUMN_FAMILIY);
	    descriptor.addFamily(labelColumnFamily);
	    HColumnDescriptor edgesColumnFamily = new HColumnDescriptor(EDGES_COLUMN_FAMILY);
	    descriptor.addFamily(edgesColumnFamily);
	    HColumnDescriptor propertiesColumnFamily = new HColumnDescriptor(PROPERTIES_COLUMN_FAMILY);
	    descriptor.addFamily(propertiesColumnFamily);
	    admin.createTable(descriptor);
	}
    }

    private void assureLabelsTablePresence(Admin admin) throws IOException {
	if (!admin.isTableAvailable(TableName.valueOf(LABELS_TABLE_NAME))) {
	    HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(LABELS_TABLE_NAME));
	    HColumnDescriptor indexColumnFamily = new HColumnDescriptor(INDEX_COLUMN_FAMILY_BYTES);
	    descriptor.addFamily(indexColumnFamily);
	    admin.createTable(descriptor);
	    try (Table table = openMetaDataTable()) {
		Put put = new Put(VERTEXID_COLUMN_BYTES);
		put.addColumn(METADATA_COLUMN_FAMILIY_BYTES, VERTEXID_COLUMN_BYTES, Bytes.toBytes(0l));
		table.put(put);
	    }
	}
    }

    private void assurePropertiesTablePresence(Admin admin) throws IOException {
	if (!admin.isTableAvailable(TableName.valueOf(PROPERTIES_TABLE_NAME))) {
	    HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(PROPERTIES_TABLE_NAME));
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

    private Table openMetaDataTable() throws IOException {
	return connection.getTable(TableName.valueOf(METADATA_TABLE_NAME));
    }

    private Table openVertexTable() throws IOException {
	return connection.getTable(TableName.valueOf(VERTICES_TABLE_NAME));
    }

    private Table openPropertyTable() throws IOException {
	return connection.getTable(TableName.valueOf(PROPERTIES_TABLE_NAME));
    }

    private Table openLabelTable() throws IOException {
	return connection.getTable(TableName.valueOf(LABELS_TABLE_NAME));
    }

    static final byte[] encodeVertexId(Object id) {
	return Bytes.toBytes(id.toString());
    }

    static final Object decodeVertexId(byte[] id) {
	return Bytes.toString(id);
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
	Put outPut = new Put(encodeVertexId(startVertex.getId()));
	outPut.addColumn(EDGES_COLUMN_FAMILY_BYTES, new EdgeKey(Direction.OUT, edgeId, edgeType).encode(), edgeValue);
	Put inPut = new Put(encodeVertexId(targetVertex.getId()));
	inPut.addColumn(EDGES_COLUMN_FAMILY_BYTES, new EdgeKey(Direction.IN, edgeId, edgeType).encode(), edgeValue);
	getCurrentTransaction().put(VERTICES_TABLE_NAME, outPut);
	getCurrentTransaction().put(VERTICES_TABLE_NAME, inPut);
	return new DuctileDBEdgeImpl(edgeType, (DuctileDBVertex) startVertex, (DuctileDBVertex) targetVertex);
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
	byte[] id = encodeVertexId(vertexId);
	Put put = new Put(id);
	put.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, DUCTILEDB_ID_PROPERTY_BYTES,
		SerializationUtils.serialize(vertexId));
	List<Put> labelIndex = new ArrayList<>();
	for (String label : labels) {
	    Put index = new Put(Bytes.toBytes(label));
	    index.addColumn(INDEX_COLUMN_FAMILY_BYTES, id, Bytes.toBytes(0));
	    labelIndex.add(index);
	    put.addColumn(LABELS_COLUMN_FAMILIY_BYTES, Bytes.toBytes(label), new byte[] { 0 });
	}
	List<Put> propertyIndex = new ArrayList<>();
	for (Entry<String, Object> property : properties.entrySet()) {
	    Put index = new Put(Bytes.toBytes(property.getKey()));
	    index.addColumn(INDEX_COLUMN_FAMILY_BYTES, id,
		    SerializationUtils.serialize((Serializable) property.getValue()));
	    propertyIndex.add(index);
	    put.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, Bytes.toBytes(property.getKey()),
		    SerializationUtils.serialize((Serializable) property.getValue()));
	}
	getCurrentTransaction().put(VERTICES_TABLE_NAME, put);
	getCurrentTransaction().put(LABELS_TABLE_NAME, labelIndex);
	getCurrentTransaction().put(PROPERTIES_TABLE_NAME, propertyIndex);
	properties.put(DUCTILEDB_ID_PROPERTY, vertexId);
	return new DuctileDBVertexImpl(this, id, labels, properties);
    }

    @Override
    public DuctileDBEdge getEdge(Object edgeId) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public Iterable<Edge> getEdges() {
	throw new UnsupportedOperationException("This operation is not supported.");
    }

    @Override
    public Iterable<Edge> getEdges(String propertyKey, Object propertyValue) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public Features getFeatures() {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public DuctileDBVertex getVertex(Object vertexId) {
	try (Table vertexTable = openVertexTable()) {
	    byte[] id = encodeVertexId(vertexId);
	    Get get = new Get(id);
	    Result result = vertexTable.get(get);
	    if (result.isEmpty()) {
		return null;
	    }
	    // Reading labels...
	    Set<String> labels = new HashSet<>();
	    NavigableMap<byte[], byte[]> labelsMap = result.getFamilyMap(LABELS_COLUMN_FAMILIY_BYTES);
	    if (labelsMap != null) {
		for (byte[] label : labelsMap.keySet()) {
		    labels.add(Bytes.toString(label));
		}
	    }
	    // Reading properties...
	    Map<String, Object> properties = new HashMap<>();
	    NavigableMap<byte[], byte[]> propertyMap = result.getFamilyMap(PROPERTIES_COLUMN_FAMILY_BYTES);
	    if (propertyMap != null) {
		for (Entry<byte[], byte[]> entry : propertyMap.entrySet()) {
		    String key = Bytes.toString(entry.getKey());
		    Object value = SerializationUtils.deserialize(entry.getValue());
		    properties.put(key, value);
		}
	    }
	    // TODO read edges
	    return new DuctileDBVertexImpl(this, id, labels, properties);
	} catch (IOException e) {
	    throw new DuctileDBException("Could not get vertex.", e);
	}
    }

    @Override
    public Iterable<Vertex> getVertices() {
	throw new UnsupportedOperationException("This operation is not supported.");
    }

    @Override
    public Iterable<Vertex> getVertices(String propertyKey, Object propertyValue) {
	try (Table table = openPropertyTable()) {
	    List<Vertex> vertices = new ArrayList<>();
	    Get get = new Get(Bytes.toBytes(propertyKey));
	    get.addFamily(INDEX_COLUMN_FAMILY_BYTES);
	    Result result = table.get(get);
	    NavigableMap<byte[], byte[]> propertyMap = result.getFamilyMap(INDEX_COLUMN_FAMILY_BYTES);
	    if (propertyMap != null) {
		for (Entry<byte[], byte[]> entry : propertyMap.entrySet()) {
		    Object value = SerializationUtils.deserialize(entry.getValue());
		    if (propertyValue.equals(value)) {
			Object key = decodeVertexId(entry.getKey());
			vertices.add(getVertex(key));
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
	try (Table table = openLabelTable()) {
	    List<DuctileDBVertex> vertices = new ArrayList<>();
	    Get get = new Get(Bytes.toBytes(label));
	    get.addFamily(INDEX_COLUMN_FAMILY_BYTES);
	    Result result = table.get(get);
	    NavigableMap<byte[], byte[]> propertyMap = result.getFamilyMap(INDEX_COLUMN_FAMILY_BYTES);
	    if (propertyMap != null) {
		for (byte[] vertexId : propertyMap.keySet()) {
		    vertices.add(getVertex(vertexId));
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
	throw new UnsupportedOperationException("Querying of HGraph is not supported, yet.");
    }

    @Override
    public void removeEdge(Edge edge) {
	Delete deleteOutEdge = new Delete(encodeVertexId(edge.getVertex(Direction.OUT).getId()));
	deleteOutEdge.addColumns(EDGES_COLUMN_FAMILY_BYTES,
		new EdgeKey(Direction.OUT, edge.getId(), edge.getLabel()).encode());
	getCurrentTransaction().delete(VERTICES_TABLE_NAME, deleteOutEdge);
	Delete deleteInEdge = new Delete(encodeVertexId(edge.getVertex(Direction.IN).getId()));
	deleteInEdge.addColumns(EDGES_COLUMN_FAMILY_BYTES,
		new EdgeKey(Direction.IN, edge.getId(), edge.getLabel()).encode());
	getCurrentTransaction().delete(VERTICES_TABLE_NAME, deleteInEdge);
    }

    @Override
    public void removeVertex(Vertex vertex) {
	byte[] vertexId = encodeVertexId(vertex.getId());
	for (String label : ((DuctileDBVertex) vertex).getLabels()) {
	    Delete delete = new Delete(Bytes.toBytes(label));
	    delete.addColumn(INDEX_COLUMN_FAMILY_BYTES, vertexId);
	    getCurrentTransaction().delete(LABELS_TABLE_NAME, delete);
	}
	for (String propertyKey : vertex.getPropertyKeys()) {
	    Delete delete = new Delete(Bytes.toBytes(propertyKey));
	    delete.addColumn(INDEX_COLUMN_FAMILY_BYTES, vertexId);
	    getCurrentTransaction().delete(PROPERTIES_TABLE_NAME, delete);
	}
	Delete delete = new Delete(vertexId);
	getCurrentTransaction().delete(VERTICES_TABLE_NAME, delete);
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

    public void addLabel(byte[] id, String label) {
	Put put = new Put(id);
	put.addColumn(LABELS_COLUMN_FAMILIY_BYTES, Bytes.toBytes(label), Bytes.toBytes(label));
	Put index = new Put(Bytes.toBytes(label));
	index.addColumn(INDEX_COLUMN_FAMILY_BYTES, id, Bytes.toBytes(0));
	getCurrentTransaction().put(VERTICES_TABLE_NAME, put);
	getCurrentTransaction().put(LABELS_TABLE_NAME, index);
    }

    public void removeLabel(byte[] id, String label) {
	Delete delete = new Delete(id);
	delete.addColumn(LABELS_COLUMN_FAMILIY_BYTES, Bytes.toBytes(label));
	Delete index = new Delete(Bytes.toBytes(label));
	index.addColumn(INDEX_COLUMN_FAMILY_BYTES, id);
	getCurrentTransaction().delete(VERTICES_TABLE_NAME, delete);
	getCurrentTransaction().delete(LABELS_TABLE_NAME, index);
    }

    void setVertexProperty(byte[] id, String key, Object value) {
	Put put = new Put(id);
	put.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, Bytes.toBytes(key),
		SerializationUtils.serialize((Serializable) value));
	Put index = new Put(Bytes.toBytes(key));
	index.addColumn(INDEX_COLUMN_FAMILY_BYTES, id, SerializationUtils.serialize((Serializable) value));
	getCurrentTransaction().put(VERTICES_TABLE_NAME, put);
	getCurrentTransaction().put(PROPERTIES_TABLE_NAME, index);
    }

    public void removeVertexProperty(byte[] id, String key) {
	Delete delete = new Delete(id);
	delete.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, Bytes.toBytes(key));
	Delete index = new Delete(Bytes.toBytes(key));
	index.addColumn(INDEX_COLUMN_FAMILY_BYTES, id);
	getCurrentTransaction().delete(VERTICES_TABLE_NAME, delete);
	getCurrentTransaction().delete(PROPERTIES_TABLE_NAME, index);
    }
}
