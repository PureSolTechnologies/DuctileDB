package com.puresoltechnologies.ductiledb.core.graph.schema;

import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngine;
import com.puresoltechnologies.ductiledb.storage.engine.io.Bytes;
import com.puresoltechnologies.ductiledb.storage.engine.schema.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManager;
import com.puresoltechnologies.ductiledb.storage.engine.schema.TableDescriptor;

public class GraphSchema {

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

    private final DatabaseEngine storageEngine;

    public GraphSchema(DatabaseEngine storageEngine) {
	super();
	this.storageEngine = storageEngine;
    }

    public void checkAndCreateEnvironment() throws SchemaException, StorageException {
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

    private NamespaceDescriptor assureNamespacePresence(SchemaManager schemaManager)
	    throws SchemaException, StorageException {
	NamespaceDescriptor namespace = schemaManager.getNamespace(DUCTILEDB_NAMESPACE);
	if (namespace == null) {
	    namespace = schemaManager.createNamespace(DUCTILEDB_NAMESPACE);
	}
	return namespace;
    }

    private void assureMetaDataTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws SchemaException, StorageException {
	if (schemaManager.getTable(namespace, DatabaseTable.METADATA.getName()) == null) {
	    TableDescriptor tableDescription = schemaManager.createTable(namespace, DatabaseTable.METADATA.getName());
	    schemaManager.createColumnFamily(tableDescription, DatabaseColumnFamily.METADATA.getNameBytes());
	    schemaManager.createColumnFamily(tableDescription, DatabaseColumnFamily.VARIABLES.getNameBytes());

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
	    throws SchemaException, StorageException {
	if (schemaManager.getTable(namespace, DatabaseTable.PROPERTY_DEFINITIONS.getName()) == null) {
	    TableDescriptor table = schemaManager.createTable(namespace, DatabaseTable.PROPERTY_DEFINITIONS.getName());
	    schemaManager.createColumnFamily(table, DatabaseColumnFamily.VERTEX_DEFINITION.getNameBytes());
	    schemaManager.createColumnFamily(table, DatabaseColumnFamily.EDGE_DEFINITION.getNameBytes());
	}
    }

    private void assureTypesTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws SchemaException, StorageException {
	if (schemaManager.getTable(namespace, DatabaseTable.TYPE_DEFINITIONS.getName()) == null) {
	    TableDescriptor table = schemaManager.createTable(namespace, DatabaseTable.TYPE_DEFINITIONS.getName());
	    schemaManager.createColumnFamily(table, DatabaseColumnFamily.VERTEX_DEFINITION.getNameBytes());
	    schemaManager.createColumnFamily(table, DatabaseColumnFamily.EDGE_DEFINITION.getNameBytes());
	}
    }

    private void assureVerticesTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws SchemaException, StorageException {
	if (schemaManager.getTable(namespace, DatabaseTable.VERTICES.getName()) == null) {
	    TableDescriptor table = schemaManager.createTable(namespace, DatabaseTable.VERTICES.getName());
	    schemaManager.createColumnFamily(table, DatabaseColumnFamily.TYPES.getNameBytes());
	    schemaManager.createColumnFamily(table, DatabaseColumnFamily.EDGES.getNameBytes());
	    schemaManager.createColumnFamily(table, DatabaseColumnFamily.PROPERTIES.getNameBytes());
	}
    }

    private void assureEdgesTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws SchemaException, StorageException {
	if (schemaManager.getTable(namespace, DatabaseTable.EDGES.getName()) == null) {
	    TableDescriptor table = schemaManager.createTable(namespace, DatabaseTable.EDGES.getName());
	    schemaManager.createColumnFamily(table, DatabaseColumnFamily.TYPES.getNameBytes());
	    schemaManager.createColumnFamily(table, DatabaseColumnFamily.PROPERTIES.getNameBytes());
	    schemaManager.createColumnFamily(table, DatabaseColumnFamily.VERICES.getNameBytes());
	}
    }

    private void assureVertexTypesIndexTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws SchemaException, StorageException {
	if (schemaManager.getTable(namespace, DatabaseTable.VERTEX_TYPES.getName()) == null) {
	    TableDescriptor table = schemaManager.createTable(namespace, DatabaseTable.VERTEX_TYPES.getName());
	    schemaManager.createColumnFamily(table, DatabaseColumnFamily.INDEX.getNameBytes());
	}
    }

    private void assureVertexPropertiesIndexTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws SchemaException, StorageException {
	if (schemaManager.getTable(namespace, DatabaseTable.VERTEX_PROPERTIES.getName()) == null) {
	    TableDescriptor table = schemaManager.createTable(namespace, DatabaseTable.VERTEX_PROPERTIES.getName());
	    schemaManager.createColumnFamily(table, DatabaseColumnFamily.INDEX.getNameBytes());
	}
    }

    private void assureEdgeTypesIndexTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws SchemaException, StorageException {
	if (schemaManager.getTable(namespace, DatabaseTable.EDGE_TYPES.getName()) == null) {
	    TableDescriptor table = schemaManager.createTable(namespace, DatabaseTable.EDGE_TYPES.getName());
	    schemaManager.createColumnFamily(table, DatabaseColumnFamily.INDEX.getNameBytes());
	}
    }

    private void assureEdgePropertiesIndexTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws SchemaException, StorageException {
	if (schemaManager.getTable(namespace, DatabaseTable.EDGE_PROPERTIES.getName()) == null) {
	    TableDescriptor table = schemaManager.createTable(namespace, DatabaseTable.EDGE_PROPERTIES.getName());
	    schemaManager.createColumnFamily(table, DatabaseColumnFamily.INDEX.getNameBytes());
	}
    }

}
