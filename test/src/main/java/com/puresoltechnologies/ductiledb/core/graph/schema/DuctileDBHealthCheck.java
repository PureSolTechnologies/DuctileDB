package com.puresoltechnologies.ductiledb.core.graph.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.NavigableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.core.graph.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.graph.EdgeDirection;
import com.puresoltechnologies.ductiledb.core.graph.GraphStoreImpl;
import com.puresoltechnologies.ductiledb.core.graph.utils.IdEncoder;
import com.puresoltechnologies.ductiledb.core.graph.utils.Serializer;
import com.puresoltechnologies.ductiledb.core.tables.ExecutionException;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.dml.DataManipulationLanguage;
import com.puresoltechnologies.ductiledb.core.tables.dml.PreparedSelect;
import com.puresoltechnologies.ductiledb.core.tables.schema.TableStoreSchema;
import com.puresoltechnologies.ductiledb.engine.DatabaseEngine;
import com.puresoltechnologies.ductiledb.engine.Get;
import com.puresoltechnologies.ductiledb.engine.Key;
import com.puresoltechnologies.ductiledb.engine.Result;
import com.puresoltechnologies.ductiledb.engine.ResultScanner;
import com.puresoltechnologies.ductiledb.engine.Scan;
import com.puresoltechnologies.ductiledb.engine.TableEngine;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnMap;
import com.puresoltechnologies.ductiledb.engine.cf.ColumnValue;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;

/**
 * This class is used to check for consistency in DuctileDB. It is primarily
 * used for testing to check consistency for indices, duplicated storage and so
 * forth.
 * 
 * <b>Attention:</b> The check is quite expensive and includes a duplicated read
 * of the whole database. It is for small databases during testing, but not for
 * productive use.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class DuctileDBHealthCheck {

    private static final Logger logger = LoggerFactory.getLogger(DuctileDBHealthCheck.class);

    public static void runCheck(GraphStoreImpl graph) throws IOException, StorageException {
	new DuctileDBHealthCheck(graph).runCheck();
    }

    public static void runCheckForEmpty(GraphStoreImpl graph) throws IOException {
	new DuctileDBHealthCheck(graph).runCheckForEmpty();
    }

    private final GraphStoreImpl graph;
    private final DatabaseEngine storageEngine;
    private final String namespace;

    public DuctileDBHealthCheck(GraphStoreImpl graph) throws IOException {
	super();
	this.graph = graph;
	storageEngine = graph.getStorageEngine();
	namespace = graph.getConfiguration().getNamespace();
    }

    public void runCheck() throws IOException, StorageException {
	checkForCorrectVertexIndices();
	checkForCorrectEdgeIndices();
	checkVertexTypeIndex();
	checkVertexPropertyIndex();
	checkEdgeTypeIndex();
	checkEdgePropertyIndex();
    }

    /**
     * This method reads all vertices and checks the correct presence of all
     * indices.
     * 
     * @throws IOException
     */
    private void checkForCorrectVertexIndices() throws IOException {
	logger.info("Check vertices...");
	Iterable<DuctileDBVertex> vertices = graph.getVertices();
	for (DuctileDBVertex vertex : vertices) {
	    logger.info("Checking '" + vertex + "'...");
	    assertEquals(vertex, graph.getVertex(vertex.getId()));
	    TableEngine vertexTypesTable = storageEngine.getTable(namespace, DatabaseTable.VERTEX_TYPES.getName());
	    for (String type : vertex.getTypes()) {
		Result result = vertexTypesTable.get(new Get(Key.of(type)));
		assertFalse("Could not find row for type '" + type + "' in vertex type index.", result.isEmpty());
		ColumnValue value = result.getFamilyMap(DatabaseColumnFamily.INDEX.getKey())
			.get(Key.of(IdEncoder.encodeRowId(vertex.getId())));
		assertNotNull("Could not find vertex type index entry for type '" + type + "' for vertex with id '"
			+ vertex.getId() + "'", value);
	    }

	    TableEngine vertexPropertiesTable = storageEngine.getTable(namespace,
		    DatabaseTable.VERTEX_PROPERTIES.getName());
	    for (String key : vertex.getPropertyKeys()) {
		Object value = vertex.getProperty(key);
		Result result = vertexPropertiesTable.get(new Get(Key.of(key)));
		assertFalse("Could not find row for property '" + key + "' in vertex property index.",
			result.isEmpty());
		ColumnMap familyMap = result.getFamilyMap(DatabaseColumnFamily.INDEX.getKey());
		assertNotNull("Could not find vertex property index entry for property '" + key
			+ "' for vertex with id '" + vertex.getId() + "'", familyMap);
		Object deserialized = Serializer.deserializePropertyValue(
			familyMap.get(Key.of(IdEncoder.encodeRowId(vertex.getId()))).getBytes());
		assertEquals("Value for vertex property '" + key + "' for vertex '" + vertex.getId()
			+ "' is not as expected. ", value, deserialized);
	    }

	    for (DuctileDBEdge edge : vertex.getEdges(EdgeDirection.BOTH)) {
		DuctileDBEdge readEdge = graph.getEdge(edge.getId());
		assertEquals("Separately stored edge is not equals.", edge, readEdge);
	    }
	}
	logger.info("Vertices checked.");

    }

    /**
     * This method reads all edges and checks the correct presence of all
     * indices.
     * 
     * @throws IOException
     */
    private void checkForCorrectEdgeIndices() throws IOException {
	logger.info("Check edges...");
	Iterable<DuctileDBEdge> edges = graph.getEdges();
	for (DuctileDBEdge edge : edges) {
	    assertEquals(edge, graph.getEdge(edge.getId()));
	    String type = edge.getType();
	    TableEngine table = storageEngine.getTable(namespace, DatabaseTable.EDGE_TYPES.getName());
	    Result result = table.get(new Get(Key.of(type)));
	    assertFalse("Could not find row for type '" + type + "' in edge type index.", result.isEmpty());
	    ColumnValue value = result.getFamilyMap(DatabaseColumnFamily.INDEX.getKey())
		    .get(Key.of(IdEncoder.encodeRowId(edge.getId())));
	    assertNotNull("Could not find edge type index entry for type '" + type + "' for edge with id '"
		    + edge.getId() + "'", value);

	    table = storageEngine.getTable(namespace, DatabaseTable.EDGE_PROPERTIES.getName());
	    for (String key : edge.getPropertyKeys()) {
		Object propertyValue = edge.getProperty(key);
		Result tableResult = table.get(new Get(Key.of(key)));
		assertFalse("Could not find row for property '" + key + "' in edge property index.",
			tableResult.isEmpty());
		NavigableMap<Key, ColumnValue> familyMap = tableResult
			.getFamilyMap(DatabaseColumnFamily.INDEX.getKey());
		assertNotNull("Could not find edge property index entry for property '" + key + "' for edge with id '"
			+ edge.getId() + "'", familyMap);
		Object deserialized = Serializer.deserializePropertyValue(
			familyMap.get(Key.of(IdEncoder.encodeRowId(edge.getId()))).getBytes());
		assertEquals(
			"Value for edge property '" + key + "' for edge '" + edge.getId() + "' is not as expected. ",
			propertyValue, deserialized);
	    }

	    DuctileDBVertex startVertex = edge.getStartVertex();
	    assertNotNull("Start vertex is not present.", startVertex);
	    DuctileDBVertex targetVertex = edge.getTargetVertex();
	    assertNotNull("Target vertex is not present.", targetVertex);
	}
	logger.info("Edges checked.");
    }

    /**
     * This method reads the complete vertex type index and checks for the
     * correct values and to not have stray entries.
     */
    private void checkVertexTypeIndex() {
	// TODO Auto-generated method stub

    }

    /**
     * This method reads the complete vertex property index and checks for the
     * correct values and to not have stray entries.
     */
    private void checkVertexPropertyIndex() {
	// TODO Auto-generated method stub

    }

    /**
     * This method reads the complete edge type index and checks for the correct
     * values and to not have stray entries.
     */
    private void checkEdgeTypeIndex() {
	// TODO Auto-generated method stub

    }

    /**
     * This method reads the complete edge property index and checks for the
     * correct values and to not have stray entries.
     */
    private void checkEdgePropertyIndex() {
	// TODO Auto-generated method stub
    }

    public void runCheckForEmpty() throws IOException {
	assertFalse("Vertices were found, but graph is expected to be empty.",
		graph.getVertices().iterator().hasNext());
	assertFalse("Edges were found, but graph is expected to be empty.", graph.getEdges().iterator().hasNext());
	for (DatabaseTable schemaTable : DatabaseTable.values()) {
	    if (schemaTable == DatabaseTable.METADATA) {
		/*
		 * The metadata table is allowed to contain data.
		 */
		continue;
	    }
	    TableEngine table = storageEngine.getTable(namespace, schemaTable.getName());
	    ResultScanner scanner = table.getScanner(new Scan());
	    assertNull("Row data was found, but database was expected to be empty. Row found in table '"
		    + schemaTable.name() + "'.", scanner.next());
	}
    }

    public static void runCheckForEmpty(TableStoreImpl tableStore) throws ExecutionException {
	DataManipulationLanguage dml = tableStore.getDataManipulationLanguage();
	PreparedSelect select = dml.prepareSelect(TableStoreSchema.SYSTEM_NAMESPACE_NAME,
		com.puresoltechnologies.ductiledb.core.tables.schema.DatabaseTable.TABLES.getName());
	select.bind().execute(tableStore);
    }
}
