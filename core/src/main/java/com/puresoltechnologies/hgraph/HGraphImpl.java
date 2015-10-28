package com.puresoltechnologies.hgraph;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
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

import com.puresoltechnologies.hgraph.tx.HGraphTransaction;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;

public class HGraphImpl implements HGraph {

    static final String NAMESPACE_NAME = "hgraph";
    static final String VERTICES_TABLE_NAME = NAMESPACE_NAME + ":vertices";
    static final String LABELS_COLUMN_FAMILIY = "labels";
    static final byte[] LABELS_COLUMN_FAMILIY_BYTES = Bytes.toBytes(LABELS_COLUMN_FAMILIY);
    static final String EDGES_COLUMN_FAMILY = "edges";
    static final byte[] EDGES_COLUMN_FAMILY_BYTES = Bytes.toBytes(EDGES_COLUMN_FAMILY);
    static final String PROPERTIES_COLUMN_FAMILY = "properties";
    static final byte[] PROPERTIES_COLUMN_FAMILY_BYTES = Bytes.toBytes(PROPERTIES_COLUMN_FAMILY);
    static final String HGRAPH_ID_PROPERTY = "hgraph.id";
    static final byte[] HGRAPH_ID_PROPERTY_BYTES = Bytes.toBytes(HGRAPH_ID_PROPERTY);

    private final ThreadLocal<HGraphTransaction> transactions = new ThreadLocal<HGraphTransaction>() {
	@Override
	protected HGraphTransaction initialValue() {
	    return new HGraphTransaction();
	}
    };

    private final Connection connection;

    public HGraphImpl(Connection connection) throws IOException {
	this.connection = connection;
	checkAndCreateEnvironment();
	openVertexTable();
    }

    private void checkAndCreateEnvironment() throws IOException {
	try (Admin admin = connection.getAdmin()) {
	    assureNamespacePresence(admin);
	    assureVertexTablePresence(admin);
	}
    }

    private void assureVertexTablePresence(Admin admin) throws IOException {
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

    @Override
    public void close() throws IOException {
	connection.close();
    }

    @Override
    public Connection getConnection() {
	return connection;
    }

    private HGraphTransaction getCurrentTransaction() {
	return transactions.get();
    }

    private Table openVertexTable() throws IOException {
	return connection.getTable(TableName.valueOf(VERTICES_TABLE_NAME));
    }

    static final byte[] encodeKey(Object id) {
	return Bytes.toBytes(id.toString());
    }

    static final Object decodeRowKey(byte[] id) {
	return Bytes.toString(id);
    }

    @Override
    public HGraphEdge addEdge(Object edgeId, Vertex startVertex, Vertex targetVertex, String edgeType) {
	return addEdge(edgeId, startVertex, targetVertex, edgeType, new HashMap<>());
    }

    @Override
    public HGraphEdge addEdge(Object edgeId, Vertex startVertex, Vertex targetVertex, String edgeType,
	    Map<String, Object> properties) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public HGraphVertex addVertex(Object vertexId) {
	return addVertex(vertexId, new HashSet<>(), new HashMap<>());
    }

    @Override
    public HGraphVertex addVertex(Object vertexId, Set<String> labels, Map<String, Object> properties) {
	byte[] id = encodeKey(vertexId);
	Put put = new Put(id);
	put.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, HGRAPH_ID_PROPERTY_BYTES,
		SerializationUtils.serialize((Serializable) vertexId));
	for (String label : labels) {
	    put.addColumn(LABELS_COLUMN_FAMILIY_BYTES, Bytes.toBytes(label), new byte[] { 0 });
	}
	for (Entry<String, Object> property : properties.entrySet()) {
	    put.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, Bytes.toBytes(property.getKey()),
		    SerializationUtils.serialize((Serializable) property.getValue()));
	}
	getCurrentTransaction().put(VERTICES_TABLE_NAME, put);
	properties.put(HGRAPH_ID_PROPERTY, vertexId);
	return new HGraphVertexImpl(this, id, labels, properties);
    }

    @Override
    public Edge getEdge(Object edgeId) {
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
    public HGraphVertex getVertex(Object vertexId) {
	try (Table vertexTable = openVertexTable()) {
	    byte[] id = encodeKey(vertexId);
	    Get get = new Get(id);
	    Result result = vertexTable.get(get);
	    if (result.isEmpty()) {
		return null;
	    }
	    // TODO read edges and labels
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
	    return new HGraphVertexImpl(this, id, labels, properties);
	} catch (IOException e) {
	    throw new HGraphException("Could not get vertex.", e);
	}
    }

    @Override
    public Iterable<Vertex> getVertices() {
	throw new UnsupportedOperationException("This operation is not supported.");
    }

    @Override
    public Iterable<Vertex> getVertices(String propertyKey, Object propertyValue) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public GraphQuery query() {
	throw new UnsupportedOperationException("Querying of HGraph is not supported, yet.");
    }

    @Override
    public void removeEdge(Edge edge) {
	Delete deleteOutEdge = new Delete(encodeKey(edge.getVertex(Direction.OUT).getId()));
	deleteOutEdge.addColumns(EDGES_COLUMN_FAMILY_BYTES, encodeKey(edge.getId()));
	getCurrentTransaction().delete(VERTICES_TABLE_NAME, deleteOutEdge);
	Delete deleteInEdge = new Delete(encodeKey(edge.getVertex(Direction.IN).getId()));
	deleteInEdge.addColumns(EDGES_COLUMN_FAMILY_BYTES, encodeKey(edge.getId()));
	getCurrentTransaction().delete(VERTICES_TABLE_NAME, deleteInEdge);
    }

    @Override
    public void removeVertex(Vertex vertex) {
	Delete delete = new Delete(encodeKey(vertex.getId()));
	getCurrentTransaction().delete(VERTICES_TABLE_NAME, delete);
    }

    @Override
    public void shutdown() {
	try {
	    close();
	} catch (IOException e) {
	    throw new HGraphException("Could not shutdown graph.", e);
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
	getCurrentTransaction().put(VERTICES_TABLE_NAME, put);
    }

    public void removeLabel(byte[] id, String label) {
	Delete delete = new Delete(id);
	delete.addColumn(LABELS_COLUMN_FAMILIY_BYTES, Bytes.toBytes(label));
	getCurrentTransaction().delete(VERTICES_TABLE_NAME, delete);
    }

    void setVertexProperty(byte[] id, String key, Object value) {
	Put put = new Put(id);
	put.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, Bytes.toBytes(key),
		SerializationUtils.serialize((Serializable) value));
	getCurrentTransaction().put(VERTICES_TABLE_NAME, put);
    }

    public void removeVertexProperty(byte[] id, String key) {
	Delete delete = new Delete(id);
	delete.addColumn(PROPERTIES_COLUMN_FAMILY_BYTES, Bytes.toBytes(key));
	getCurrentTransaction().delete(VERTICES_TABLE_NAME, delete);
    }
}
