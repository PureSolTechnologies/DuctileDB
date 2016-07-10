package com.puresoltechnologies.ductiledb.core.graph.schema;

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

import com.puresoltechnologies.ductiledb.core.utils.BuildInformation;

public class HBaseSchema {

    public static final String DUCTILEDB_NAMESPACE = "ductiledb";

    public static final String PROPERTY_TYPE_COLUMN = "PropertyType";
    public static final byte[] PROPERTY_TYPE_COLUMN_BYTES = Bytes.toBytes(PROPERTY_TYPE_COLUMN);

    public static final String ELEMENT_TYPE_COLUMN = "ElementType";
    public static final byte[] ELEMENT_TYPE_COLUMN_BYTES = Bytes.toBytes(ELEMENT_TYPE_COLUMN);

    public static final String UNIQUENESS_COLUMN = "unique";
    public static final byte[] UNIQUENESS_COLUMN_BYTES = Bytes.toBytes(UNIQUENESS_COLUMN);

    public static final String ID_ROW = "IdRow";
    public static final byte[] ID_ROW_BYTES = Bytes.toBytes(ID_ROW);

    public static final String DUCTILEDB_ID_PROPERTY = "~ductiledb.id";
    public static final String DUCTILEDB_CREATE_TIMESTAMP_PROPERTY = "~ductiledb.timestamp.created";

    private final Connection connection;

    public HBaseSchema(Connection connection) {
	super();
	this.connection = connection;
    }

    public void checkAndCreateEnvironment() throws IOException {
	try (Admin admin = connection.getAdmin()) {
	    assureNamespacePresence(admin);
	    assureMetaDataTablePresence(admin);
	    assurePropertiesTablePresence(admin);
	    assureTypesTablePresence(admin);
	    assureVerticesTablePresence(admin);
	    assureEdgesTablePresence(admin);
	    assureVertexTypesIndexTablePresence(admin);
	    assureVertexPropertiesIndexTablePresence(admin);
	    assureEdgeTypesIndexTablePresence(admin);
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
	if (!admin.isTableAvailable(HBaseTable.METADATA.getTableName())) {
	    HTableDescriptor descriptor = new HTableDescriptor(HBaseTable.METADATA.getTableName());
	    HColumnDescriptor metaDataColumnFamily = new HColumnDescriptor(HBaseColumnFamily.METADATA.getNameBytes());
	    descriptor.addFamily(metaDataColumnFamily);
	    HColumnDescriptor graphVariableColumnFamily = new HColumnDescriptor(
		    HBaseColumnFamily.VARIABLES.getNameBytes());
	    descriptor.addFamily(graphVariableColumnFamily);
	    admin.createTable(descriptor);
	    try (Table table = connection.getTable(HBaseTable.METADATA.getTableName());) {
		Put vertexIdPut = new Put(HBaseColumn.VERTEX_ID.getNameBytes());
		vertexIdPut.addColumn(HBaseColumnFamily.METADATA.getNameBytes(), HBaseColumn.VERTEX_ID.getNameBytes(),
			Bytes.toBytes(0l));
		Put edgeIdPut = new Put(HBaseColumn.EDGE_ID.getNameBytes());
		edgeIdPut.addColumn(HBaseColumnFamily.METADATA.getNameBytes(), HBaseColumn.EDGE_ID.getNameBytes(),
			Bytes.toBytes(0l));
		Put schemaVersionPut = new Put(HBaseColumn.SCHEMA_VERSION.getNameBytes());
		String version = BuildInformation.getVersion();
		if (version.startsWith("${")) {
		    // fallback for test environments, but backed up by test.
		    version = "0.1.0";
		}
		schemaVersionPut.addColumn(HBaseColumnFamily.METADATA.getNameBytes(),
			HBaseColumn.SCHEMA_VERSION.getNameBytes(), Bytes.toBytes(version));
		table.put(Arrays.asList(vertexIdPut, edgeIdPut, schemaVersionPut));
	    }
	}
    }

    private void assurePropertiesTablePresence(Admin admin) throws IOException {
	if (!admin.isTableAvailable(HBaseTable.PROPERTY_DEFINITIONS.getTableName())) {
	    HTableDescriptor descriptor = new HTableDescriptor(HBaseTable.PROPERTY_DEFINITIONS.getTableName());
	    descriptor.addFamily(new HColumnDescriptor(HBaseColumnFamily.VERTEX_DEFINITION.getNameBytes()));
	    descriptor.addFamily(new HColumnDescriptor(HBaseColumnFamily.EDGE_DEFINITION.getNameBytes()));
	    admin.createTable(descriptor);
	}
    }

    private void assureTypesTablePresence(Admin admin) throws IOException {
	if (!admin.isTableAvailable(HBaseTable.TYPE_DEFINITIONS.getTableName())) {
	    HTableDescriptor descriptor = new HTableDescriptor(HBaseTable.TYPE_DEFINITIONS.getTableName());
	    descriptor.addFamily(new HColumnDescriptor(HBaseColumnFamily.VERTEX_DEFINITION.getNameBytes()));
	    descriptor.addFamily(new HColumnDescriptor(HBaseColumnFamily.EDGE_DEFINITION.getNameBytes()));
	    admin.createTable(descriptor);
	}
    }

    private void assureVerticesTablePresence(Admin admin) throws IOException {
	if (!admin.isTableAvailable(HBaseTable.VERTICES.getTableName())) {
	    HTableDescriptor descriptor = new HTableDescriptor(HBaseTable.VERTICES.getTableName());
	    HColumnDescriptor typeColumnFamily = new HColumnDescriptor(HBaseColumnFamily.TYPES.getNameBytes());
	    descriptor.addFamily(typeColumnFamily);
	    HColumnDescriptor edgesColumnFamily = new HColumnDescriptor(HBaseColumnFamily.EDGES.getNameBytes());
	    descriptor.addFamily(edgesColumnFamily);
	    HColumnDescriptor propertiesColumnFamily = new HColumnDescriptor(
		    HBaseColumnFamily.PROPERTIES.getNameBytes());
	    descriptor.addFamily(propertiesColumnFamily);
	    admin.createTable(descriptor);
	}
    }

    private void assureEdgesTablePresence(Admin admin) throws IOException {
	if (!admin.isTableAvailable(HBaseTable.EDGES.getTableName())) {
	    HTableDescriptor descriptor = new HTableDescriptor(HBaseTable.EDGES.getTableName());
	    HColumnDescriptor typesColumnFamily = new HColumnDescriptor(HBaseColumnFamily.TYPES.getNameBytes());
	    descriptor.addFamily(typesColumnFamily);
	    HColumnDescriptor propertiesColumnFamily = new HColumnDescriptor(
		    HBaseColumnFamily.PROPERTIES.getNameBytes());
	    descriptor.addFamily(propertiesColumnFamily);
	    HColumnDescriptor verticesColumnFamily = new HColumnDescriptor(HBaseColumnFamily.VERICES.getNameBytes());
	    descriptor.addFamily(verticesColumnFamily);
	    admin.createTable(descriptor);
	}
    }

    private void assureVertexTypesIndexTablePresence(Admin admin) throws IOException {
	if (!admin.isTableAvailable(HBaseTable.VERTEX_TYPES.getTableName())) {
	    HTableDescriptor descriptor = new HTableDescriptor(HBaseTable.VERTEX_TYPES.getTableName());
	    HColumnDescriptor indexColumnFamily = new HColumnDescriptor(HBaseColumnFamily.INDEX.getNameBytes());
	    descriptor.addFamily(indexColumnFamily);
	    admin.createTable(descriptor);
	}
    }

    private void assureVertexPropertiesIndexTablePresence(Admin admin) throws IOException {
	if (!admin.isTableAvailable(HBaseTable.VERTEX_PROPERTIES.getTableName())) {
	    HTableDescriptor descriptor = new HTableDescriptor(HBaseTable.VERTEX_PROPERTIES.getTableName());
	    HColumnDescriptor indexColumnFamily = new HColumnDescriptor(HBaseColumnFamily.INDEX.getNameBytes());
	    descriptor.addFamily(indexColumnFamily);
	    admin.createTable(descriptor);
	}
    }

    private void assureEdgeTypesIndexTablePresence(Admin admin) throws IOException {
	if (!admin.isTableAvailable(HBaseTable.EDGE_TYPES.getTableName())) {
	    HTableDescriptor descriptor = new HTableDescriptor(HBaseTable.EDGE_TYPES.getTableName());
	    HColumnDescriptor indexColumnFamily = new HColumnDescriptor(HBaseColumnFamily.INDEX.getNameBytes());
	    descriptor.addFamily(indexColumnFamily);
	    admin.createTable(descriptor);
	}
    }

    private void assureEdgePropertiesIndexTablePresence(Admin admin) throws IOException {
	if (!admin.isTableAvailable(HBaseTable.EDGE_PROPERTIES.getTableName())) {
	    HTableDescriptor descriptor = new HTableDescriptor(HBaseTable.EDGE_PROPERTIES.getTableName());
	    HColumnDescriptor indexColumnFamily = new HColumnDescriptor(HBaseColumnFamily.INDEX.getNameBytes());
	    descriptor.addFamily(indexColumnFamily);
	    admin.createTable(descriptor);
	}
    }

}
