package com.puresoltechnologies.ductiledb.core;

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

import com.puresoltechnologies.ductiledb.api.Vertex;
import com.puresoltechnologies.ductiledb.core.AbstractDuctileDBGraphTest;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.core.DuctileDBHealthCheck;

public class DuctileDBVertexIT extends AbstractDuctileDBGraphTest {

    private static int NUMBER = 1000;
    private static DuctileDBHealthCheck healthChecker;

    @BeforeClass
    public static void initializeHealthCheck() throws IOException {
	healthChecker = new DuctileDBHealthCheck((DuctileDBGraphImpl) graph);
    }

    @Before
    public void checkHealth() throws IOException {
	healthChecker.runCheck();
    }

    @AfterClass
    public static void checkFinalHealth() throws IOException {
	healthChecker.runCheck();
    }

    @Test
    public void testCreateAndRemoveVertex() throws IOException {
	Vertex vertex = graph.addVertex();
	graph.commit();
	healthChecker.runCheck();

	Vertex vertex2 = graph.getVertex(vertex.getId());
	assertEquals(vertex, vertex2);

	graph.removeVertex(vertex);
	graph.commit();
	healthChecker.runCheck();

	assertNull(graph.getVertex(vertex.getId()));
	vertex.remove();
	graph.commit();
    }

    @Test
    public void testSetAndRemoveVertexProperty() throws IOException {
	Vertex vertex = graph.addVertex();
	vertex.setProperty("property1", "value1");
	graph.commit();
	healthChecker.runCheck();
	assertEquals("value1", vertex.getProperty("property1"));

	Vertex vertex2 = graph.getVertex(vertex.getId());
	assertEquals(vertex, vertex2);

	vertex.removeProperty("property1");
	graph.commit();
	healthChecker.runCheck();
	assertNull(graph.getVertex(vertex.getId()).getProperty("property1"));

	vertex.remove();
	graph.commit();
    }

    @Test
    public void testSetAndRemoveVertexLabel() throws IOException {
	Vertex vertex = graph.addVertex();
	vertex.addLabel("label");
	graph.commit();
	healthChecker.runCheck();
	assertTrue(vertex.hasLabel("label"));

	Iterator<String> iterator = vertex.getLabels().iterator();
	assertTrue(iterator.hasNext());
	assertEquals("label", iterator.next());
	assertFalse(iterator.hasNext());

	Vertex vertex2 = graph.getVertex(vertex.getId());
	assertEquals(vertex, vertex2);
	assertTrue(vertex2.hasLabel("label"));
	iterator = vertex2.getLabels().iterator();
	assertTrue(iterator.hasNext());
	assertEquals("label", iterator.next());
	assertFalse(iterator.hasNext());

	vertex.removeLabel("label");
	graph.commit();
	healthChecker.runCheck();
	assertFalse(vertex.hasLabel("label"));
	assertFalse(graph.getVertex(vertex.getId()).hasLabel("label"));
	vertex.remove();
	graph.commit();
    }

    @Test
    public void testVertexCreationPerformance() throws IOException {
	Set<Vertex> vertices = new HashSet<>();
	long start = System.currentTimeMillis();
	for (int i = 0; i < NUMBER; ++i) {
	    Vertex vertex = graph.addVertex();
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
	for (Vertex vertex : vertices) {
	    vertex.remove();
	}
	graph.commit();
    }

    @Test
    public void testFullVertexCreationPerformance() throws IOException {
	Set<Vertex> vertices = new HashSet<>();
	long start = System.currentTimeMillis();
	for (int i = 0; i < NUMBER; ++i) {
	    Set<String> labels = new HashSet<>();
	    Map<String, Object> properties = new HashMap<>();
	    for (int j = 0; j < 10; ++j) {
		labels.add("label" + j);
		properties.put("property" + j, j);
	    }
	    Vertex vertex = graph.addVertex(labels, properties);
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
	for (Vertex vertex : vertices) {
	    vertex.remove();
	}
	graph.commit();
    }

    @Test
    public void testSetPropertyPerformance() throws IOException {
	Vertex vertex = graph.addVertex();
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
    public void testSetLabelPerformance() throws IOException {
	Vertex vertex = graph.addVertex();
	long start = System.currentTimeMillis();
	for (int i = 0; i < NUMBER; ++i) {
	    vertex.addLabel("label" + i);
	    assertTrue(vertex.hasLabel("label" + i));
	}
	graph.commit();
	long stop = System.currentTimeMillis();
	long duration = stop - start;
	double speed = (double) NUMBER / (double) duration * 1000.0;
	System.out.println("time: " + duration + "ms");
	System.out.println("speed: " + speed + " labels/s");
	System.out.println("speed: " + 1000 / speed + " ms/label");
	assertTrue(duration < 10000);
	vertex.remove();
	graph.commit();
    }
}
