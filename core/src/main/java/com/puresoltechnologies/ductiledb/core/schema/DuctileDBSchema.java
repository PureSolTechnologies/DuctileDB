package com.puresoltechnologies.ductiledb.core.schema;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class DuctileDBSchema {

    public static final String DUCTILEDB_NAMESPACE = "ductiledb";

    public static final String METADATA_COLUMN_FAMILIY = "metadata";
    public static final byte[] METADATA_COLUMN_FAMILIY_BYTES = Bytes.toBytes(METADATA_COLUMN_FAMILIY);

    public static final String ID_ROW = "IdRow";
    public static final byte[] ID_ROW_BYTES = Bytes.toBytes(ID_ROW);

    public static final String VERTEXID_COLUMN = "VertexId";
    public static final byte[] VERTEXID_COLUMN_BYTES = Bytes.toBytes(VERTEXID_COLUMN);

    public static final String START_VERTEXID_COLUMN = "StartVertexId";
    public static final byte[] START_VERTEXID_COLUMN_BYTES = Bytes.toBytes(START_VERTEXID_COLUMN);

    public static final String TARGET_VERTEXID_COLUMN = "TargetVertexId";
    public static final byte[] TARGET_VERTEXID_COLUMN_BYTES = Bytes.toBytes(TARGET_VERTEXID_COLUMN);

    public static final String EDGEID_COLUMN = "EdgeId";
    public static final byte[] EDGEID_COLUMN_BYTES = Bytes.toBytes(EDGEID_COLUMN);

    public static final String LABELS_COLUMN_FAMILIY = "labels";
    public static final byte[] LABELS_COLUMN_FAMILIY_BYTES = Bytes.toBytes(LABELS_COLUMN_FAMILIY);

    public static final String EDGES_COLUMN_FAMILY = "edges";
    public static final byte[] EDGES_COLUMN_FAMILY_BYTES = Bytes.toBytes(EDGES_COLUMN_FAMILY);

    public static final String PROPERTIES_COLUMN_FAMILY = "properties";
    public static final byte[] PROPERTIES_COLUMN_FAMILY_BYTES = Bytes.toBytes(PROPERTIES_COLUMN_FAMILY);

    public static final String VERICES_COLUMN_FAMILY = "vertices";
    public static final byte[] VERICES_COLUMN_FAMILY_BYTES = Bytes.toBytes(VERICES_COLUMN_FAMILY);

    public static final String INDEX_COLUMN_FAMILY = "index";
    public static final byte[] INDEX_COLUMN_FAMILY_BYTES = Bytes.toBytes(INDEX_COLUMN_FAMILY);

    public static final String DUCTILEDB_ID_PROPERTY = ".ductiledb.id";
    public static final String DUCTILEDB_CREATE_TIMESTAMP_PROPERTY = ".ductiledb.timestamp.created";

    private final Connection connection;

    public DuctileDBSchema(Connection connection) {
	super();
	this.connection = connection;
    }

    public void checkAndCreateEnvironment() throws IOException {
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
	if (!admin.isTableAvailable(SchemaTable.METADATA.getTableName())) {
	    HTableDescriptor descriptor = new HTableDescriptor(SchemaTable.METADATA.getTableName());
	    HColumnDescriptor metaDataColumnFamily = new HColumnDescriptor(METADATA_COLUMN_FAMILIY);
	    descriptor.addFamily(metaDataColumnFamily);
	    admin.createTable(descriptor);
	    try (Table table = connection.getTable(SchemaTable.METADATA.getTableName());) {
		Put vertexIdPut = new Put(VERTEXID_COLUMN_BYTES);
		vertexIdPut.addColumn(METADATA_COLUMN_FAMILIY_BYTES, VERTEXID_COLUMN_BYTES, Bytes.toBytes(0l));
		Put edgeIdPut = new Put(EDGEID_COLUMN_BYTES);
		edgeIdPut.addColumn(METADATA_COLUMN_FAMILIY_BYTES, EDGEID_COLUMN_BYTES, Bytes.toBytes(0l));
		table.put(Arrays.asList(vertexIdPut, edgeIdPut));
	    }
	}
    }

    private void assureVerticesTablePresence(Admin admin) throws IOException {
	if (!admin.isTableAvailable(SchemaTable.VERTICES.getTableName())) {
	    HTableDescriptor descriptor = new HTableDescriptor(SchemaTable.VERTICES.getTableName());
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
	if (!admin.isTableAvailable(SchemaTable.EDGES.getTableName())) {
	    HTableDescriptor descriptor = new HTableDescriptor(SchemaTable.EDGES.getTableName());
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
	if (!admin.isTableAvailable(SchemaTable.VERTEX_LABELS.getTableName())) {
	    HTableDescriptor descriptor = new HTableDescriptor(SchemaTable.VERTEX_LABELS.getTableName());
	    HColumnDescriptor indexColumnFamily = new HColumnDescriptor(INDEX_COLUMN_FAMILY_BYTES);
	    descriptor.addFamily(indexColumnFamily);
	    admin.createTable(descriptor);
	}
    }

    private void assureVertexPropertiesIndexTablePresence(Admin admin) throws IOException {
	if (!admin.isTableAvailable(SchemaTable.VERTEX_PROPERTIES.getTableName())) {
	    HTableDescriptor descriptor = new HTableDescriptor(SchemaTable.VERTEX_PROPERTIES.getTableName());
	    HColumnDescriptor indexColumnFamily = new HColumnDescriptor(INDEX_COLUMN_FAMILY_BYTES);
	    descriptor.addFamily(indexColumnFamily);
	    admin.createTable(descriptor);
	}
    }

    private void assureEdgeLabelsIndexTablePresence(Admin admin) throws IOException {
	if (!admin.isTableAvailable(SchemaTable.EDGE_LABELS.getTableName())) {
	    HTableDescriptor descriptor = new HTableDescriptor(SchemaTable.EDGE_LABELS.getTableName());
	    HColumnDescriptor indexColumnFamily = new HColumnDescriptor(INDEX_COLUMN_FAMILY_BYTES);
	    descriptor.addFamily(indexColumnFamily);
	    admin.createTable(descriptor);
	}
    }

    private void assureEdgePropertiesIndexTablePresence(Admin admin) throws IOException {
	if (!admin.isTableAvailable(SchemaTable.EDGE_PROPERTIES.getTableName())) {
	    HTableDescriptor descriptor = new HTableDescriptor(SchemaTable.EDGE_PROPERTIES.getTableName());
	    HColumnDescriptor indexColumnFamily = new HColumnDescriptor(INDEX_COLUMN_FAMILY_BYTES);
	    descriptor.addFamily(indexColumnFamily);
	    admin.createTable(descriptor);
	}
    }

}
