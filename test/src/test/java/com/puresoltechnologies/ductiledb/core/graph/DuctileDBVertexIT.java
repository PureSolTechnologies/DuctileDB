package com.puresoltechnologies.ductiledb.core.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.puresoltechnologies.ductiledb.api.graph.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.graph.schema.DuctileDBHealthCheck;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;

public class DuctileDBVertexIT extends AbstractDuctileDBGraphTest {

    private static int NUMBER = 1000;
    private static DuctileDBGraphImpl graph;
    private static DuctileDBHealthCheck healthChecker;

    @BeforeClass
    public static void initializeHealthCheck() throws IOException {
	graph = getGraph();
	healthChecker = new DuctileDBHealthCheck(graph);
    }

    @Before
    public void checkHealth() throws IOException, StorageException {
	healthChecker.runCheck();
    }

    @AfterClass
    public static void checkFinalHealth() throws IOException, StorageException {
	if (healthChecker != null) {
	    healthChecker.runCheck();
	}
    }

    @Test
    public void testCreateAndRemoveVertex() throws IOException {
	DuctileDBVertex vertex = graph.addVertex();
	graph.commit();

	DuctileDBVertex vertex2 = graph.getVertex(vertex.getId());
	assertEquals(vertex, vertex2);

	graph.removeVertex(vertex);
	graph.commit();

	assertNotInTransaction(vertex);
    }

    @Test
    public void testSetAndRemoveVertexProperty() throws IOException {
	DuctileDBVertex vertex = graph.addVertex();
	vertex.setProperty("property1", "value1");
	graph.commit();
	assertEquals("value1", vertex.getProperty("property1"));

	DuctileDBVertex vertex2 = graph.getVertex(vertex.getId());
	assertEquals(vertex, vertex2);

	vertex.removeProperty("property1");
	graph.commit();
	assertNull(graph.getVertex(vertex.getId()).getProperty("property1"));

	vertex.remove();
	graph.commit();
    }

    @Test
    public void testSetAndRemoveVertexType() throws IOException {
	DuctileDBVertex vertex = graph.addVertex();
	vertex.addType("type");
	graph.commit();
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
	assertFalse(vertex.hasType("type"));
	assertFalse(graph.getVertex(vertex.getId()).hasType("type"));
	vertex.remove();
	graph.commit();
    }

    @Test
    public void testPropertyCRUD() {
	DuctileDBGraphImpl graph = getGraph();
	assertEquals(0, DuctileDBTestHelper.count(graph.getVertices()));

	DuctileDBVertex vertex = graph.addVertex();
	graph.commit();
	assertEquals(1, DuctileDBTestHelper.count(graph.getVertices()));
	DuctileDBVertex readVertex = graph.getVertices().iterator().next();
	assertEquals(vertex, readVertex);

	vertex.setProperty("key", "value");
	assertEquals("value", vertex.getProperty("key"));
	graph.commit();
	readVertex = graph.getVertices().iterator().next();
	assertEquals("value", readVertex.getProperty("key"));

	vertex.setProperty("key", "value2");
	assertEquals("value2", vertex.getProperty("key"));
	graph.commit();
	readVertex = graph.getVertices().iterator().next();
	assertEquals("value2", readVertex.getProperty("key"));

	vertex.removeProperty("key");
	graph.commit();
	readVertex = graph.getVertices().iterator().next();
	assertNull(readVertex.getProperty("key"));
    }

    @Test
    public void testVertexCreationPerformance() throws IOException {
	Set<DuctileDBVertex> vertices = new HashSet<>();
	long start = System.currentTimeMillis();
	for (int i = 0; i < NUMBER; ++i) {
	    DuctileDBVertex vertex = graph.addVertex();
	    vertices.add(vertex);
	}
	graph.commit();
	long stop = System.currentTimeMillis();
	long duration = stop - start;
	double speed = (double) NUMBER / (double) duration * 1000.0;
	System.out.println("time: " + duration + "ms");
	System.out.println("speed: " + speed + " vertices/s");
	System.out.println("speed: " + 1000 / speed + " ms/vertex");
	assertTrue(duration < 10000);
	for (DuctileDBVertex vertex : vertices) {
	    vertex.remove();
	}
	graph.commit();
    }

    @Test
    public void testFullVertexCreationPerformance() throws IOException {
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
	long stop = System.currentTimeMillis();
	long duration = stop - start;
	double speed = (double) NUMBER / (double) duration * 1000.0;
	System.out.println("time: " + duration + "ms");
	System.out.println("speed: " + speed + " full_vertices/s");
	System.out.println("speed: " + 1000 / speed + " ms/full_vertex");
	assertTrue(duration < 10000);
	for (DuctileDBVertex vertex : vertices) {
	    vertex.remove();
	}
	graph.commit();
    }

    @Test
    public void testSetPropertyPerformance() throws IOException {
	DuctileDBVertex vertex = graph.addVertex();
	long start = System.currentTimeMillis();
	for (int i = 0; i < NUMBER; ++i) {
	    vertex.setProperty("key" + i, i);
	    assertEquals(i, ((Integer) vertex.getProperty("key" + i)).intValue());
	}
	graph.commit();
	long stop = System.currentTimeMillis();
	long duration = stop - start;
	double speed = (double) NUMBER / (double) duration * 1000.0;
	System.out.println("time: " + duration + "ms");
	System.out.println("speed: " + speed + " properties/s");
	System.out.println("speed: " + 1000 / speed + " ms/property");
	assertTrue(duration < 10000);
	vertex.remove();
	graph.commit();
    }

    @Test
    public void testAddTypePerformance() throws IOException {
	DuctileDBVertex vertex = graph.addVertex();
	long start = System.currentTimeMillis();
	for (int i = 0; i < NUMBER; ++i) {
	    vertex.addType("type" + i);
	    assertTrue(vertex.hasType("type" + i));
	}
	graph.commit();
	long stop = System.currentTimeMillis();
	long duration = stop - start;
	double speed = (double) NUMBER / (double) duration * 1000.0;
	System.out.println("time: " + duration + "ms");
	System.out.println("speed: " + speed + " types/s");
	System.out.println("speed: " + 1000 / speed + " ms/type");
	assertTrue(duration < 10000);
	vertex.remove();
	graph.commit();
    }
}
