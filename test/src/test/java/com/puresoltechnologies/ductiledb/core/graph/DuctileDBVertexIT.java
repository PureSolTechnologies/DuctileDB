package com.puresoltechnologies.ductiledb.core.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.puresoltechnologies.ductiledb.api.graph.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.graph.schema.DuctileDBHealthCheck;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;

public class DuctileDBVertexIT extends AbstractDuctileDBGraphTest {

    private static int NUMBER = 90;
    private static DuctileDBGraphImpl graph;
    private static DuctileDBHealthCheck healthChecker;

    @BeforeClass
    public static void initializeHealthCheck() throws IOException {
	graph = getGraph();
	healthChecker = new DuctileDBHealthCheck(graph);
    }

    @Before
    public void checkHealthBefore() throws IOException, StorageException {
	healthChecker.runCheck();
    }

    @After
    public void checkHealthAfterwards() throws IOException, StorageException {
	healthChecker.runCheck();
    }

    @Test
    public void testCreateAndRemoveSimpleVertex() throws IOException, StorageException {
	DuctileDBVertex vertex = graph.addVertex();
	graph.commit();
	healthChecker.runCheck();

	DuctileDBVertex vertex2 = graph.getVertex(vertex.getId());
	assertEquals(vertex, vertex2);

	graph.removeVertex(vertex);
	graph.commit();
	healthChecker.runCheck();

	assertNotInTransaction(vertex);
    }

    @Test
    public void testCreateAndRemoveFullVertex() throws IOException, StorageException {
	Iterable<DuctileDBVertex> vertices = graph.getVertices();
	assertFalse(vertices.iterator().hasNext());
	DuctileDBVertex vertex = graph.addVertex();
	vertices = graph.getVertices();
	assertTrue(vertices.iterator().hasNext());

	vertex.addType("type");
	for (int i = 0; i < 195; ++i) {
	    vertex.setProperty("property" + i, "value" + i);
	}
	graph.commit();
	vertices = graph.getVertices();
	assertTrue(vertices.iterator().hasNext());
	healthChecker.runCheck();

	DuctileDBVertex vertex2 = graph.getVertex(vertex.getId());
	assertEquals(vertex, vertex2);

	vertex.remove();
	graph.commit();
	vertices = graph.getVertices();
	assertFalse(vertices.iterator().hasNext());
	healthChecker.runCheck();

	assertNotInTransaction(vertex);
    }

    @Test
    public void testSetAndRemoveVertexProperty() throws IOException, StorageException {
	DuctileDBVertex vertex = graph.addVertex();
	vertex.setProperty("property1", "value1");
	graph.commit();
	healthChecker.runCheck();
	assertEquals("value1", vertex.getProperty("property1"));

	DuctileDBVertex vertex2 = graph.getVertex(vertex.getId());
	assertEquals(vertex, vertex2);

	vertex.removeProperty("property1");
	graph.commit();
	healthChecker.runCheck();
	assertNull(graph.getVertex(vertex.getId()).getProperty("property1"));

	vertex.remove();
	graph.commit();
	healthChecker.runCheck();
    }

    @Test
    public void testSetAndRemoveVertexType() throws IOException, StorageException {
	DuctileDBVertex vertex = graph.addVertex();
	vertex.addType("type");
	graph.commit();
	healthChecker.runCheck();
	graph.runCompaction();
	healthChecker.runCheck();
	assertTrue(vertex.hasType("type"));

	Iterator<String> iterator = vertex.getTypes().iterator();
	assertTrue(iterator.hasNext());
	assertEquals("type", iterator.next());
	assertFalse(iterator.hasNext());

	DuctileDBVertex vertex2 = graph.getVertex(vertex.getId());
	assertEquals(vertex, vertex2);
	assertTrue(vertex2.hasType("type"));
	iterator = vertex2.getTypes().iterator();
	assertTrue(iterator.hasNext());
	assertEquals("type", iterator.next());
	assertFalse(iterator.hasNext());

	vertex.removeType("type");
	graph.commit();
	healthChecker.runCheck();
	graph.runCompaction();
	healthChecker.runCheck();

	assertFalse(vertex.hasType("type"));
	assertFalse(graph.getVertex(vertex.getId()).hasType("type"));
	vertex.remove();
	graph.commit();
	DuctileDBHealthCheck.runCheckForEmpty(graph);
	healthChecker.runCheck();
	graph.runCompaction();
	healthChecker.runCheck();
    }

    @Test
    public void testPropertyCRUD() throws IOException, StorageException {
	DuctileDBGraphImpl graph = getGraph();
	assertEquals(0, DuctileDBTestHelper.count(graph.getVertices()));

	DuctileDBVertex vertex = graph.addVertex();
	graph.commit();
	healthChecker.runCheck();

	assertEquals(1, DuctileDBTestHelper.count(graph.getVertices()));
	DuctileDBVertex readVertex = graph.getVertices().iterator().next();
	assertEquals(vertex, readVertex);

	vertex.setProperty("key", "value");
	assertEquals("value", vertex.getProperty("key"));
	graph.commit();
	healthChecker.runCheck();

	readVertex = graph.getVertices().iterator().next();
	assertEquals("value", readVertex.getProperty("key"));

	vertex.setProperty("key", "value2");
	assertEquals("value2", vertex.getProperty("key"));
	graph.commit();
	healthChecker.runCheck();

	readVertex = graph.getVertices().iterator().next();
	assertEquals("value2", readVertex.getProperty("key"));

	vertex.removeProperty("key");
	graph.commit();
	healthChecker.runCheck();

	readVertex = graph.getVertices().iterator().next();
	assertNull(readVertex.getProperty("key"));
    }

    @Test
    public void testVertexCreationPerformance() throws IOException, StorageException {
	Set<DuctileDBVertex> vertices = new HashSet<>();
	long start = System.currentTimeMillis();
	for (int i = 0; i < NUMBER; ++i) {
	    DuctileDBVertex vertex = graph.addVertex();
	    vertices.add(vertex);
	}
	graph.commit();
	healthChecker.runCheck();

	long stop = System.currentTimeMillis();
	long duration = stop - start;
	double speed = (double) NUMBER / (double) duration * 1000.0;
	System.out.println("time: " + duration + "ms");
	System.out.println("speed: " + speed + " vertices/s");
	System.out.println("speed: " + 1000 / speed + " ms/vertex");
	assertTrue(speed > 1000);
	for (DuctileDBVertex vertex : vertices) {
	    vertex.remove();
	}
	graph.commit();
	healthChecker.runCheck();
    }

    @Test
    public void testFullVertexCreationPerformance() throws IOException, StorageException {
	DatabaseEngineImpl storageEngine = graph.getStorageEngine();
	storageEngine.setRunCompactions(true);
	Set<DuctileDBVertex> vertices = new HashSet<>();
	long start = System.currentTimeMillis();
	for (int i = 0; i < NUMBER; ++i) {
	    Set<String> types = new HashSet<>();
	    Map<String, Object> properties = new HashMap<>();
	    for (int j = 0; j < 10; ++j) {
		types.add("type" + j);
		properties.put("property" + j, j);
	    }
	    DuctileDBVertex vertex = graph.addVertex(types, properties);
	    vertices.add(vertex);
	}
	graph.commit();
	healthChecker.runCheck();

	long stop = System.currentTimeMillis();
	long duration = stop - start;
	double speed = (double) NUMBER / (double) duration * 1000.0;
	System.out.println("time: " + duration + "ms");
	System.out.println("speed: " + speed + " full_vertices/s");
	System.out.println("speed: " + 1000 / speed + " ms/full_vertex");
	for (DuctileDBVertex vertex : vertices) {
	    vertex.remove();
	}
	graph.commit();
	healthChecker.runCheck();
	assertTrue(speed > 100);
    }

    @Test
    public void testSetPropertyPerformance() throws IOException, StorageException {
	Iterable<DuctileDBVertex> vertices = graph.getVertices();
	assertNotNull(vertices);
	Iterator<DuctileDBVertex> verticesIterator = vertices.iterator();
	assertNotNull(verticesIterator);
	assertFalse(verticesIterator.hasNext());

	DuctileDBVertex vertex = graph.addVertex();
	long start = System.currentTimeMillis();
	for (int i = 0; i < NUMBER; ++i) {
	    vertex.setProperty("key" + i, i);
	    assertEquals(i, ((Integer) vertex.getProperty("key" + i)).intValue());
	}
	graph.commit();
	healthChecker.runCheck();

	DuctileDBVertex readVertex = graph.getVertex(vertex.getId());
	assertNotNull(readVertex);
	assertEquals(vertex, readVertex);

	vertices = graph.getVertices();
	assertNotNull(vertices);
	verticesIterator = vertices.iterator();
	assertNotNull(verticesIterator);
	assertTrue(verticesIterator.hasNext());
	assertEquals(vertex, verticesIterator.next());
	assertFalse(verticesIterator.hasNext());

	long stop = System.currentTimeMillis();
	long duration = stop - start;
	double speed = (double) NUMBER / (double) duration * 1000.0;
	System.out.println("time: " + duration + "ms");
	System.out.println("speed: " + speed + " properties/s");
	System.out.println("speed: " + 1000 / speed + " ms/property");
	assertTrue(speed > 1000);
	vertex.remove();
	graph.commit();
	healthChecker.runCheck();
    }

    @Test
    public void testAddTypePerformance() throws IOException, StorageException {
	DuctileDBVertex vertex = graph.addVertex();
	long start = System.currentTimeMillis();
	for (int i = 0; i < NUMBER; ++i) {
	    vertex.addType("type" + i);
	    assertTrue(vertex.hasType("type" + i));
	}
	graph.commit();
	healthChecker.runCheck();

	long stop = System.currentTimeMillis();
	long duration = stop - start;
	double speed = (double) NUMBER / (double) duration * 1000.0;
	System.out.println("time: " + duration + "ms");
	System.out.println("speed: " + speed + " types/s");
	System.out.println("speed: " + 1000 / speed + " ms/type");
	assertTrue(speed > 1000);
	vertex.remove();
	graph.commit();
	healthChecker.runCheck();
    }
}
