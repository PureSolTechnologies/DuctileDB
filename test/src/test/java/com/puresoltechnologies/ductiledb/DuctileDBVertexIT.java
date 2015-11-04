package com.puresoltechnologies.ductiledb;

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
import org.junit.BeforeClass;
import org.junit.Test;

import com.tinkerpop.blueprints.Vertex;

public class DuctileDBVertexIT extends AbstractDuctileDBTest {

    private static int NUMBER = 1000;

    private static DuctileDBGraph graph;

    @BeforeClass
    public static void connect() throws IOException {
	graph = GraphFactory.createGraph();
    }

    @AfterClass
    public static void disconnect() throws IOException {
	graph.close();
    }

    @Test
    public void testCreateAndRemoveVertex() throws IOException {
	Vertex vertex = graph.addVertex();
	graph.commit();

	Vertex vertex2 = graph.getVertex(vertex.getId());
	assertEquals(vertex, vertex2);

	graph.removeVertex(vertex);
	graph.commit();
	assertNull(graph.getVertex(vertex.getId()));
    }

    @Test
    public void testSetAndRemoveVertexProperty() throws IOException {
	Vertex vertex = graph.addVertex();
	vertex.setProperty("property1", "value1");
	graph.commit();
	assertEquals("value1", vertex.getProperty("property1"));

	Vertex vertex2 = graph.getVertex(vertex.getId());
	assertEquals(vertex, vertex2);

	vertex.removeProperty("property1");
	graph.commit();
	assertNull(graph.getVertex(vertex.getId()).getProperty("property1"));
    }

    @Test
    public void testSetAndRemoveVertexLabel() throws IOException {
	DuctileDBVertex vertex = graph.addVertex();
	vertex.addLabel("label");
	graph.commit();
	assertTrue(vertex.hasLabel("label"));

	Iterator<String> iterator = vertex.getLabels().iterator();
	assertTrue(iterator.hasNext());
	assertEquals("label", iterator.next());
	assertFalse(iterator.hasNext());

	DuctileDBVertex vertex2 = graph.getVertex(vertex.getId());
	assertEquals(vertex, vertex2);
	assertTrue(vertex2.hasLabel("label"));
	iterator = vertex2.getLabels().iterator();
	assertTrue(iterator.hasNext());
	assertEquals("label", iterator.next());
	assertFalse(iterator.hasNext());

	vertex.removeLabel("label");
	graph.commit();
	assertFalse(vertex.hasLabel("label"));
	assertFalse(graph.getVertex(vertex.getId()).hasLabel("label"));
    }

    @Test
    public void testVertexCreationPerformance() throws IOException {
	long start = System.currentTimeMillis();
	for (int i = 0; i < NUMBER; ++i) {
	    graph.addVertex();
	}
	graph.commit();
	long stop = System.currentTimeMillis();
	long duration = stop - start;
	double speed = (double) NUMBER / (double) duration * 1000.0;
	System.out.println("time: " + duration + "ms");
	System.out.println("speed: " + speed + " vertices/s");
	System.out.println("speed: " + 1000 / speed + " ms/vertex");
	assertTrue(duration < 10000);
    }

    @Test
    public void testFullVertexCreationPerformance() throws IOException {
	long start = System.currentTimeMillis();
	for (int i = 0; i < NUMBER; ++i) {
	    Set<String> labels = new HashSet<>();
	    Map<String, Object> properties = new HashMap<>();
	    for (int j = 0; j < 10; ++j) {
		labels.add("label" + j);
		properties.put("property" + j, j);
	    }
	    graph.addVertex(labels, properties);
	}
	graph.commit();
	long stop = System.currentTimeMillis();
	long duration = stop - start;
	double speed = (double) NUMBER / (double) duration * 1000.0;
	System.out.println("time: " + duration + "ms");
	System.out.println("speed: " + speed + " full_vertices/s");
	System.out.println("speed: " + 1000 / speed + " ms/full_vertex");
	assertTrue(duration < 10000);
    }

    @Test
    public void testSetPropertyPerformance() throws IOException {
	Vertex vertex = graph.addVertex("PropertyPerformanceTest");
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
    }

    @Test
    public void testSetLabelPerformance() throws IOException {
	DuctileDBVertex vertex = graph.addVertex("LabelPerformanceTest");
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
    }

}