package com.puresoltechnologies.ductiledb.core.graph.schema;

import com.puresoltechnologies.ductiledb.storage.engine.StorageEngine;
import com.puresoltechnologies.ductiledb.storage.engine.schema.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManager;
import com.puresoltechnologies.ductiledb.storage.engine.schema.TableDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.utils.Bytes;

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

    private final StorageEngine storageEngine;

    public HBaseSchema(StorageEngine storageEngine) {
	super();
	this.storageEngine = storageEngine;
    }

    public void checkAndCreateEnvironment() throws SchemaException {
	SchemaManager schemaManager = storageEngine.getSchemaManager();
	NamespaceDescriptor namespace = assureNamespacePresence(schemaManager);
	assureMetaDataTablePresence(schemaManager, namespace);
	assurePropertiesTablePresence(schemaManager, namespace);
	assureTypesTablePresence(schemaManager, namespace);
	assureVerticesTablePresence(schemaManager, namespace);
	assureEdgesTablePresence(schemaManager, namespace);
	assureVertexTypesIndexTablePresence(schemaManager, namespace);
	assureVertexPropertiesIndexTablePresence(schemaManager, namespace);
	assureEdgeTypesIndexTablePresence(schemaManager, namespace);
	assureEdgePropertiesIndexTablePresence(schemaManager, namespace);

    }

    private NamespaceDescriptor assureNamespacePresence(SchemaManager schemaManager) throws SchemaException {
	NamespaceDescriptor namespace = schemaManager.getNamespace(DUCTILEDB_NAMESPACE);
	if (namespace == null) {
	    namespace = schemaManager.createNamespace(DUCTILEDB_NAMESPACE);
	}
	return namespace;
    }

    private void assureMetaDataTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws SchemaException {
	if (schemaManager.getTable(namespace, HBaseTable.METADATA.getName()) == null) {
	    TableDescriptor tableDescription = schemaManager.createTable(namespace, HBaseTable.METADATA.getName());
	    schemaManager.createColumnFamily(tableDescription, HBaseColumnFamily.METADATA.getName());
	    schemaManager.createColumnFamily(tableDescription, HBaseColumnFamily.VARIABLES.getName());

	    // FIXME!!!
	    // Table table =
	    // storageEngine.getTable(HBaseTable.METADATA.getName());
	    // Put vertexIdPut = new Put(HBaseColumn.VERTEX_ID.getNameBytes());
	    // vertexIdPut.addColumn(HBaseColumnFamily.METADATA.getName(),
	    // HBaseColumn.VERTEX_ID.getNameBytes(),
	    // Bytes.empty());
	    // Put edgeIdPut = new Put(HBaseColumn.EDGE_ID.getNameBytes());
	    // edgeIdPut.addColumn(HBaseColumnFamily.METADATA.getName(),
	    // HBaseColumn.EDGE_ID.getNameBytes(),
	    // Bytes.empty());
	    // Put schemaVersionPut = new
	    // Put(HBaseColumn.SCHEMA_VERSION.getNameBytes());
	    // String version = BuildInformation.getVersion();
	    // if (version.startsWith("${")) {
	    // // fallback for test environments, but backed up by test.
	    // version = "0.1.0";
	    // }
	    // schemaVersionPut.addColumn(HBaseColumnFamily.METADATA.getName(),
	    // HBaseColumn.SCHEMA_VERSION.getNameBytes(),
	    // Bytes.toBytes(version));
	    // table.put(Arrays.asList(vertexIdPut, edgeIdPut,
	    // schemaVersionPut));

	}
    }

    private void assurePropertiesTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws SchemaException {
	if (schemaManager.getTable(namespace, HBaseTable.PROPERTY_DEFINITIONS.getName()) == null) {
	    TableDescriptor table = schemaManager.createTable(namespace, HBaseTable.PROPERTY_DEFINITIONS.getName());
	    schemaManager.createColumnFamily(table, HBaseColumnFamily.VERTEX_DEFINITION.getName());
	    schemaManager.createColumnFamily(table, HBaseColumnFamily.EDGE_DEFINITION.getName());
	}
    }

    private void assureTypesTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws SchemaException {
	if (schemaManager.getTable(namespace, HBaseTable.TYPE_DEFINITIONS.getName()) == null) {
	    TableDescriptor table = schemaManager.createTable(namespace, HBaseTable.TYPE_DEFINITIONS.getName());
	    schemaManager.createColumnFamily(table, HBaseColumnFamily.VERTEX_DEFINITION.getName());
	    schemaManager.createColumnFamily(table, HBaseColumnFamily.EDGE_DEFINITION.getName());
	}
    }

    private void assureVerticesTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws SchemaException {
	if (schemaManager.getTable(namespace, HBaseTable.VERTICES.getName()) == null) {
	    TableDescriptor table = schemaManager.createTable(namespace, HBaseTable.VERTICES.getName());
	    schemaManager.createColumnFamily(table, HBaseColumnFamily.TYPES.getName());
	    schemaManager.createColumnFamily(table, HBaseColumnFamily.EDGES.getName());
	    schemaManager.createColumnFamily(table, HBaseColumnFamily.PROPERTIES.getName());
	}
    }

    private void assureEdgesTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws SchemaException {
	if (schemaManager.getTable(namespace, HBaseTable.EDGES.getName()) == null) {
	    TableDescriptor table = schemaManager.createTable(namespace, HBaseTable.EDGES.getName());
	    schemaManager.createColumnFamily(table, HBaseColumnFamily.TYPES.getName());
	    schemaManager.createColumnFamily(table, HBaseColumnFamily.PROPERTIES.getName());
	    schemaManager.createColumnFamily(table, HBaseColumnFamily.VERICES.getName());
	}
    }

    private void assureVertexTypesIndexTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws SchemaException {
	if (schemaManager.getTable(namespace, HBaseTable.VERTEX_TYPES.getName()) == null) {
	    TableDescriptor table = schemaManager.createTable(namespace, HBaseTable.VERTEX_TYPES.getName());
	    schemaManager.createColumnFamily(table, HBaseColumnFamily.INDEX.getName());
	}
    }

    private void assureVertexPropertiesIndexTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws SchemaException {
	if (schemaManager.getTable(namespace, HBaseTable.VERTEX_PROPERTIES.getName()) == null) {
	    TableDescriptor table = schemaManager.createTable(namespace, HBaseTable.VERTEX_PROPERTIES.getName());
	    schemaManager.createColumnFamily(table, HBaseColumnFamily.INDEX.getName());
	}
    }

    private void assureEdgeTypesIndexTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws SchemaException {
	if (schemaManager.getTable(namespace, HBaseTable.EDGE_TYPES.getName()) == null) {
	    TableDescriptor table = schemaManager.createTable(namespace, HBaseTable.EDGE_TYPES.getName());
	    schemaManager.createColumnFamily(table, HBaseColumnFamily.INDEX.getName());
	}
    }

    private void assureEdgePropertiesIndexTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws SchemaException {
	if (schemaManager.getTable(namespace, HBaseTable.EDGE_PROPERTIES.getName()) == null) {
	    TableDescriptor table = schemaManager.createTable(namespace, HBaseTable.EDGE_PROPERTIES.getName());
	    schemaManager.createColumnFamily(table, HBaseColumnFamily.INDEX.getName());
	}
    }

}
