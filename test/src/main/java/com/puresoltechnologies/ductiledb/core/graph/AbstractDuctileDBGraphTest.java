package com.puresoltechnologies.ductiledb.core.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.puresoltechnologies.ductiledb.core.AbstractDuctileDBTest;
import com.puresoltechnologies.ductiledb.core.DuctileDB;
import com.puresoltechnologies.ductiledb.core.graph.schema.DuctileDBHealthCheck;
import com.puresoltechnologies.ductiledb.core.graph.utils.ElementUtils;
import com.puresoltechnologies.ductiledb.core.utils.BuildInformation;
import com.puresoltechnologies.ductiledb.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;

public class AbstractDuctileDBGraphTest extends AbstractDuctileDBTest {

    private static DuctileDB ductileDB = null;
    private static GraphStoreImpl graph = null;

    @BeforeClass
    public static void connect() throws IOException, SchemaException {
	ductileDB = getDuctileDB();
	graph = (GraphStoreImpl) ductileDB.getGraph();
	// Normally meaningless, but we do nevertheless, if tests change...
	DuctileDBTestHelper.removeGraph(graph);
	DuctileDBHealthCheck.runCheckForEmpty(graph);

	String version = BuildInformation.getVersion();
	if (!version.startsWith("${")) {
	    assertEquals("Schema version is wrong.", version, graph.createGraphManager().getVersion().toString());
	}
    }

    @AfterClass
    public static void disconnect() throws IOException {
	if (graph != null) {
	    graph.close();
	    graph = null;
	}
    }

    @Before
    public final void cleanup() throws IOException, StorageException {
	DuctileDBTestHelper.removeGraph(graph);
	graph.runCompaction();
	DuctileDBHealthCheck.runCheckForEmpty(graph);
    }

    protected static GraphStoreImpl getGraph() {
	return graph;
    }

    protected static Map<String, Object> toMap(Object... keyValues) {
	Map<String, Object> map = new HashMap<>();
	if (keyValues.length % 2 != 0) {
	    throw new IllegalArgumentException("keyValues list needs an even length.");
	}
	for (int i = 0; i < keyValues.length; i = i + 2) {
	    map.put((String) keyValues[i], keyValues[i + 1]);
	}
	return map;
    }

    protected void assertInTransaction(DuctileDBVertex vertex) {
	DuctileDBVertex readVertex = graph.getVertex(vertex.getId());
	assertEquals(vertex, readVertex);
    }

    protected void assertUnequalInTransaction(DuctileDBVertex vertex) {
	assertNotNull(graph.getVertex(vertex.getId()));
    }

    protected void assertInGraph(DuctileDBVertex vertex) {
	DuctileDBVertex readVertex = graph.createTransaction().getVertex(vertex.getId());
	assertNotNull(readVertex);
	assertEquals(vertex, readVertex);
    }

    protected void assertInGraphWithDifferentEdges(DuctileDBVertex vertex) {
	DuctileDBVertex readVertex = graph.createTransaction().getVertex(vertex.getId());
	assertNotNull(readVertex);
	assertEquals(vertex.getId(), readVertex.getId());
	assertEquals(ElementUtils.getTypes(vertex), ElementUtils.getTypes(readVertex));
	assertEquals(ElementUtils.getProperties(vertex), ElementUtils.getProperties(readVertex));
    }

    protected void assertUnequalInGraph(DuctileDBVertex vertex) {
	DuctileDBVertex readVertex = graph.createTransaction().getVertex(vertex.getId());
	assertNotNull(readVertex);
	assertFalse(vertex.equals(readVertex));
    }

    protected void assertNotInGraph(DuctileDBVertex vertex) {
	assertNull(graph.createTransaction().getVertex(vertex.getId()));
    }

    protected void assertInTransaction(DuctileDBEdge edge) {
	DuctileDBEdge readEdge = graph.getEdge(edge.getId());
	assertEquals(edge, readEdge);
    }

    protected void assertUnequalInTransaction(DuctileDBEdge edge) {
	assertNotNull(graph.getEdge(edge.getId()));
    }

    protected void assertNotInTransaction(DuctileDBVertex vertex) {
	assertException(NoSuchGraphElementException.class, () -> graph.getVertex(vertex.getId()));
    }

    protected void assertNotInTransaction(DuctileDBEdge edge) {
	assertException(NoSuchGraphElementException.class, () -> graph.getEdge(edge.getId()));
    }

    protected void assertInGraph(DuctileDBEdge edge) {
	DuctileDBEdge readEdge = graph.createTransaction().getEdge(edge.getId());
	assertNotNull(readEdge);
	assertEquals(edge, readEdge);
    }

    protected void assertUnequalInGraph(DuctileDBEdge edge) {
	DuctileDBEdge readEdge = graph.createTransaction().getEdge(edge.getId());
	assertNotNull(readEdge);
	assertFalse(edge.equals(readEdge));
    }

    protected void assertNotInGraph(DuctileDBEdge edge) {
	assertNull(graph.createTransaction().getEdge(edge.getId()));
    }

    public static Consumer<GraphStore> assertVertexEdgeCounts(final int expectedVertexCount,
	    final int expectedEdgeCount) {
	return (g) -> {
	    assertEquals(expectedVertexCount, DuctileDBTestHelper.count(g.getVertices()));
	    assertEquals(expectedEdgeCount, DuctileDBTestHelper.count(g.getEdges()));
	};
    }

    protected void assertException(Class<?> clazz, Supplier<Object> function) {
	try {
	    function.get();
	    fail("A '" + clazz.getSimpleName() + "' exception was expected");
	} catch (Exception e) {
	    assertEquals(clazz, e.getClass());
	}
    }

    protected void assertException(String message, Class<?> clazz, Supplier<Object> function) {
	try {
	    function.get();
	    fail(message);
	} catch (Exception e) {
	    assertEquals(clazz, e.getClass());
	}
    }
}
