package com.puresoltechnologies.ductiledb.core.graph.schema;

import java.io.IOException;
import java.util.Arrays;

import com.puresoltechnologies.ductiledb.bigtable.BigTable;
import com.puresoltechnologies.ductiledb.bigtable.Put;
import com.puresoltechnologies.ductiledb.columnfamily.ColumnValue;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBGraphConfiguration;
import com.puresoltechnologies.ductiledb.core.utils.BuildInformation;
import com.puresoltechnologies.ductiledb.engine.DatabaseEngine;
import com.puresoltechnologies.ductiledb.engine.Namespace;
import com.puresoltechnologies.ductiledb.logstore.Key;

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

    private final DatabaseEngine storageEngine;
    private final String namespaceName;

    public GraphSchema(DatabaseEngine storageEngine, DuctileDBGraphConfiguration configuration) {
	super();
	this.storageEngine = storageEngine;
	this.namespaceName = configuration.getNamespace();
    }

    public void checkAndCreateEnvironment() throws IOException {
	Namespace namespace = assureNamespacePresence(storageEngine);
	assureMetaDataTablePresence(namespace);
	assurePropertiesTablePresence(namespace);
	assureTypesTablePresence(namespace);
	assureVerticesTablePresence(namespace);
	assureEdgesTablePresence(namespace);
	assureVertexTypesIndexTablePresence(namespace);
	assureVertexPropertiesIndexTablePresence(namespace);
	assureEdgeTypesIndexTablePresence(namespace);
	assureEdgePropertiesIndexTablePresence(namespace);
    }

    private Namespace assureNamespacePresence(DatabaseEngine storageEngine) throws IOException {
	Namespace namespace = storageEngine.getNamespace(namespaceName);
	if (namespace == null) {
	    namespace = storageEngine.addNamespace(namespaceName);
	}
	return namespace;
    }

    private void assureMetaDataTablePresence(Namespace namespace) throws IOException {
	if (!namespace.hasTable(DatabaseTable.METADATA.getName())) {
	    BigTable table = namespace.addTable(DatabaseTable.METADATA.getName(), "Contains the graph metadata.");
	    table.addColumnFamily(DatabaseColumnFamily.METADATA.getKey());
	    table.addColumnFamily(DatabaseColumnFamily.VARIABLES.getKey());

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

    private void assurePropertiesTablePresence(Namespace namespace) throws IOException {
	if (!namespace.hasTable(DatabaseTable.PROPERTY_DEFINITIONS.getName())) {
	    BigTable table = namespace.addTable(DatabaseTable.PROPERTY_DEFINITIONS.getName(),
		    "Contains the properties.");
	    table.addColumnFamily(DatabaseColumnFamily.VERTEX_DEFINITION.getKey());
	    table.addColumnFamily(DatabaseColumnFamily.EDGE_DEFINITION.getKey());
	}
    }

    private void assureTypesTablePresence(Namespace namespace) throws IOException {
	if (!namespace.hasTable(DatabaseTable.TYPE_DEFINITIONS.getName())) {
	    BigTable table = namespace.addTable(DatabaseTable.TYPE_DEFINITIONS.getName(), "Contains the types.");
	    table.addColumnFamily(DatabaseColumnFamily.VERTEX_DEFINITION.getKey());
	    table.addColumnFamily(DatabaseColumnFamily.EDGE_DEFINITION.getKey());
	}
    }

    private void assureVerticesTablePresence(Namespace namespace) throws IOException {
	if (!namespace.hasTable(DatabaseTable.VERTICES.getName())) {
	    BigTable table = namespace.addTable(DatabaseTable.VERTICES.getName(), "Contains the vertices.");
	    table.addColumnFamily(DatabaseColumnFamily.TYPES.getKey());
	    table.addColumnFamily(DatabaseColumnFamily.EDGES.getKey());
	    table.addColumnFamily(DatabaseColumnFamily.PROPERTIES.getKey());
	}
    }

    private void assureEdgesTablePresence(Namespace namespace) throws IOException {
	if (!namespace.hasTable(DatabaseTable.EDGES.getName())) {
	    BigTable table = namespace.addTable(DatabaseTable.EDGES.getName(), "Contains the edges.");
	    table.addColumnFamily(DatabaseColumnFamily.TYPES.getKey());
	    table.addColumnFamily(DatabaseColumnFamily.PROPERTIES.getKey());
	    table.addColumnFamily(DatabaseColumnFamily.VERICES.getKey());
	}
    }

    private void assureVertexTypesIndexTablePresence(Namespace namespace) throws IOException {
	if (namespace.hasTable(DatabaseTable.VERTEX_TYPES.getName())) {
	    BigTable table = namespace.addTable(DatabaseTable.VERTEX_TYPES.getName(),
		    "Contains the vertex type index.");
	    table.addColumnFamily(DatabaseColumnFamily.INDEX.getKey());
	}
    }

    private void assureVertexPropertiesIndexTablePresence(Namespace namespace) throws IOException {
	if (namespace.hasTable(DatabaseTable.VERTEX_PROPERTIES.getName())) {
	    BigTable table = namespace.addTable(DatabaseTable.VERTEX_PROPERTIES.getName(),
		    "Contains te vertex propery index.");
	    table.addColumnFamily(DatabaseColumnFamily.INDEX.getKey());
	}
    }

    private void assureEdgeTypesIndexTablePresence(Namespace namespace) throws IOException {
	if (namespace.hasTable(DatabaseTable.EDGE_TYPES.getName())) {
	    BigTable table = namespace.addTable(DatabaseTable.EDGE_TYPES.getName(), "Contains the edge type index.");
	    table.addColumnFamily(DatabaseColumnFamily.INDEX.getKey());
	}
    }

    private void assureEdgePropertiesIndexTablePresence(Namespace namespace) throws IOException {
	if (namespace.hasTable(DatabaseTable.EDGE_PROPERTIES.getName())) {
	    BigTable table = namespace.addTable(DatabaseTable.EDGE_PROPERTIES.getName(),
		    "Contains the edge property index.");
	    table.addColumnFamily(DatabaseColumnFamily.INDEX.getKey());
	}
    }

}
