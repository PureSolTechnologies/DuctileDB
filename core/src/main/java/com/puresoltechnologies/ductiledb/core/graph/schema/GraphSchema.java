package com.puresoltechnologies.ductiledb.core.graph.schema;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

import com.puresoltechnologies.ductiledb.core.graph.DuctileDBGraphConfiguration;
import com.puresoltechnologies.ductiledb.core.utils.BuildInformation;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.Put;
import com.puresoltechnologies.ductiledb.storage.engine.TableEngine;
import com.puresoltechnologies.ductiledb.storage.engine.cf.ColumnValue;
import com.puresoltechnologies.ductiledb.storage.engine.schema.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaManager;
import com.puresoltechnologies.ductiledb.storage.engine.schema.TableDescriptor;

public class GraphSchema {

    public static final String PROPERTY_TYPE_COLUMN = "PropertyType";
    public static final Key PROPERTY_TYPE_KEY = Key.of(PROPERTY_TYPE_COLUMN);

    public static final String ELEMENT_TYPE_COLUMN = "ElementType";
    public static final Key ELEMENT_TYPE_COLUMN_KEY = Key.of(ELEMENT_TYPE_COLUMN);

    public static final String UNIQUENESS_COLUMN = "unique";
    public static final Key UNIQUENESS_COLUMN_KEY = Key.of(UNIQUENESS_COLUMN);

    public static final String ID_ROW = "IdRow";
    public static final Key ID_ROW_KEY = Key.of(ID_ROW);

    public static final String DUCTILEDB_ID_PROPERTY = "~ductiledb.id";
    public static final String DUCTILEDB_CREATE_TIMESTAMP_PROPERTY = "~ductiledb.timestamp.created";

    private final Connection connection;
    private final String namespace;

    public GraphSchema(Connection connection, DuctileDBGraphConfiguration configuration) {
	super();
	this.connection = connection;
	this.namespace = configuration.getNamespace();
    }

    public void checkAndCreateEnvironment() throws SQLException {
	SchemaManager schemaManager = connection.getSchemaManager();
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
	NamespaceDescriptor namespaceDescriptor = schemaManager.getNamespace(namespace);
	if (namespaceDescriptor == null) {
	    namespaceDescriptor = schemaManager.createNamespace(namespace);
	}
	return namespaceDescriptor;
    }

    private void assureMetaDataTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws SchemaException, StorageException {
	if (schemaManager.getTable(namespace, DatabaseTable.METADATA.getName()) == null) {
	    TableDescriptor tableDescription = schemaManager.createTable(namespace, DatabaseTable.METADATA.getName(),
		    "Contains the graph metadata.");
	    schemaManager.createColumnFamily(tableDescription, DatabaseColumnFamily.METADATA.getKey());
	    schemaManager.createColumnFamily(tableDescription, DatabaseColumnFamily.VARIABLES.getKey());

	    TableEngine table = connection.getTable(namespace.getName(), DatabaseTable.METADATA.getName());
	    Put vertexIdPut = new Put(ID_ROW_KEY);
	    vertexIdPut.addColumn(DatabaseColumnFamily.METADATA.getKey(), DatabaseColumn.VERTEX_ID.getKey(),
		    ColumnValue.of(1l));
	    Put edgeIdPut = new Put(ID_ROW_KEY);
	    edgeIdPut.addColumn(DatabaseColumnFamily.METADATA.getKey(), DatabaseColumn.EDGE_ID.getKey(),
		    ColumnValue.of(1l));
	    Put schemaVersionPut = new Put(DatabaseColumn.SCHEMA_VERSION.getKey());
	    String version = BuildInformation.getVersion();
	    if (version.startsWith("${")) {
		// fallback for test environments, but backed up by test.
		version = "0.2.0";
	    }
	    schemaVersionPut.addColumn(DatabaseColumnFamily.METADATA.getKey(), DatabaseColumn.SCHEMA_VERSION.getKey(),
		    ColumnValue.of(version));
	    table.put(Arrays.asList(vertexIdPut, edgeIdPut, schemaVersionPut));
	}
    }

    private void assurePropertiesTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws SchemaException, StorageException {
	if (schemaManager.getTable(namespace, DatabaseTable.PROPERTY_DEFINITIONS.getName()) == null) {
	    TableDescriptor table = schemaManager.createTable(namespace, DatabaseTable.PROPERTY_DEFINITIONS.getName(),
		    "Contains the properties.");
	    schemaManager.createColumnFamily(table, DatabaseColumnFamily.VERTEX_DEFINITION.getKey());
	    schemaManager.createColumnFamily(table, DatabaseColumnFamily.EDGE_DEFINITION.getKey());
	}
    }

    private void assureTypesTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws SchemaException, StorageException {
	if (schemaManager.getTable(namespace, DatabaseTable.TYPE_DEFINITIONS.getName()) == null) {
	    TableDescriptor table = schemaManager.createTable(namespace, DatabaseTable.TYPE_DEFINITIONS.getName(),
		    "Contains the types.");
	    schemaManager.createColumnFamily(table, DatabaseColumnFamily.VERTEX_DEFINITION.getKey());
	    schemaManager.createColumnFamily(table, DatabaseColumnFamily.EDGE_DEFINITION.getKey());
	}
    }

    private void assureVerticesTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws SchemaException, StorageException {
	if (schemaManager.getTable(namespace, DatabaseTable.VERTICES.getName()) == null) {
	    TableDescriptor table = schemaManager.createTable(namespace, DatabaseTable.VERTICES.getName(),
		    "Contains the vertices.");
	    schemaManager.createColumnFamily(table, DatabaseColumnFamily.TYPES.getKey());
	    schemaManager.createColumnFamily(table, DatabaseColumnFamily.EDGES.getKey());
	    schemaManager.createColumnFamily(table, DatabaseColumnFamily.PROPERTIES.getKey());
	}
    }

    private void assureEdgesTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws SchemaException, StorageException {
	if (schemaManager.getTable(namespace, DatabaseTable.EDGES.getName()) == null) {
	    TableDescriptor table = schemaManager.createTable(namespace, DatabaseTable.EDGES.getName(),
		    "Contains the edges.");
	    schemaManager.createColumnFamily(table, DatabaseColumnFamily.TYPES.getKey());
	    schemaManager.createColumnFamily(table, DatabaseColumnFamily.PROPERTIES.getKey());
	    schemaManager.createColumnFamily(table, DatabaseColumnFamily.VERICES.getKey());
	}
    }

    private void assureVertexTypesIndexTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws SchemaException, StorageException {
	if (schemaManager.getTable(namespace, DatabaseTable.VERTEX_TYPES.getName()) == null) {
	    TableDescriptor table = schemaManager.createTable(namespace, DatabaseTable.VERTEX_TYPES.getName(),
		    "Contains the vertex type index.");
	    schemaManager.createColumnFamily(table, DatabaseColumnFamily.INDEX.getKey());
	}
    }

    private void assureVertexPropertiesIndexTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws SchemaException, StorageException {
	if (schemaManager.getTable(namespace, DatabaseTable.VERTEX_PROPERTIES.getName()) == null) {
	    TableDescriptor table = schemaManager.createTable(namespace, DatabaseTable.VERTEX_PROPERTIES.getName(),
		    "Contains te vertex propery index.");
	    schemaManager.createColumnFamily(table, DatabaseColumnFamily.INDEX.getKey());
	}
    }

    private void assureEdgeTypesIndexTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws SchemaException, StorageException {
	if (schemaManager.getTable(namespace, DatabaseTable.EDGE_TYPES.getName()) == null) {
	    TableDescriptor table = schemaManager.createTable(namespace, DatabaseTable.EDGE_TYPES.getName(),
		    "Contains the edge type index.");
	    schemaManager.createColumnFamily(table, DatabaseColumnFamily.INDEX.getKey());
	}
    }

    private void assureEdgePropertiesIndexTablePresence(SchemaManager schemaManager, NamespaceDescriptor namespace)
	    throws SchemaException, StorageException {
	if (schemaManager.getTable(namespace, DatabaseTable.EDGE_PROPERTIES.getName()) == null) {
	    TableDescriptor table = schemaManager.createTable(namespace, DatabaseTable.EDGE_PROPERTIES.getName(),
		    "Contains the edge property index.");
	    schemaManager.createColumnFamily(table, DatabaseColumnFamily.INDEX.getKey());
	}
    }

}
