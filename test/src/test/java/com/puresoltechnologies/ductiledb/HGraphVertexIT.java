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

import org.junit.Test;

import com.puresoltechnologies.ductiledb.AbstractHGraphTest;
import com.puresoltechnologies.ductiledb.GraphFactory;
import com.puresoltechnologies.ductiledb.HGraph;
import com.puresoltechnologies.ductiledb.HGraphVertex;
import com.tinkerpop.blueprints.Vertex;

public class HGraphVertexIT extends AbstractHGraphTest {

    @Test
    public void testVertexCreation() throws IOException {
	try (HGraph graph = GraphFactory.createGraph()) {
	    Vertex vertex = graph.addVertex("TestId");
	    graph.commit();
	    Vertex vertex2 = graph.getVertex(vertex.getId());
	    assertEquals("TestId", vertex.getId().toString());
	    assertEquals(vertex, vertex2);
	}
    }

    @Test
    public void testVertexDelete() throws IOException {
	try (HGraph graph = GraphFactory.createGraph()) {
	    Vertex vertex = graph.addVertex("DeletionTest");
	    graph.commit();

	    Vertex vertex2 = graph.getVertex(vertex.getId());
	    assertEquals("DeletionTest", vertex.getId().toString());
	    assertEquals(vertex, vertex2);

	    graph.removeVertex(vertex);
	    graph.commit();
	    assertNull(graph.getVertex(vertex.getId()));
	}
    }

    @Test
    public void testSetProperty() throws IOException {
	try (HGraph graph = GraphFactory.createGraph()) {
	    Vertex vertex = graph.addVertex("DeletionTest");
	    vertex.setProperty("property1", "value1");
	    graph.commit();
	    assertEquals("DeletionTest", vertex.getId().toString());
	    assertEquals("value1", vertex.getProperty("property1"));

	    Vertex vertex2 = graph.getVertex(vertex.getId());
	    assertEquals(vertex, vertex2);

	    vertex.removeProperty("property1");
	    graph.commit();
	    assertNull(graph.getVertex(vertex.getProperty("property1")));
	}
    }

    @Test
    public void testSetLabel() throws IOException {
	try (HGraph graph = GraphFactory.createGraph()) {
	    HGraphVertex vertex = graph.addVertex("LabelTest");
	    vertex.addLabel("label");
	    graph.commit();
	    assertEquals("LabelTest", vertex.getId().toString());
	    assertTrue(vertex.hasLabel("label"));
	    Iterable<String> labels = vertex.getLabels();
	    Iterator<String> iterator = labels.iterator();
	    assertTrue(iterator.hasNext());
	    assertEquals("label", iterator.next());
	    assertFalse(iterator.hasNext());

	    Vertex vertex2 = graph.getVertex(vertex.getId());
	    assertEquals(vertex, vertex2);

	    vertex.removeLabel("label");
	    graph.commit();
	    assertFalse(vertex.hasLabel("label"));
	    assertFalse(graph.getVertex(vertex.getId()).hasLabel("label"));
	}
    }

    @Test
    public void testVertexCreationPerformance() throws IOException {
	try (HGraph graph = GraphFactory.createGraph()) {
	    long start = System.currentTimeMillis();
	    int number = 10000;
	    for (int i = 0; i < number; ++i) {
		Vertex vertex = graph.addVertex("TestId" + i);
		assertEquals("TestId" + i, vertex.getId().toString());
	    }
	    graph.commit();
	    long stop = System.currentTimeMillis();
	    long duration = stop - start;
	    double speed = (double) number / (double) duration * 1000.0;
	    System.out.println("time: " + duration + "ms");
	    System.out.println("speed: " + speed + " vertices/s");
	    System.out.println("speed: " + 1000 / speed + " ms/vertex");
	    assertTrue(duration < 10000);
	}
    }

    @Test
    public void testFullVertexCreationPerformance() throws IOException {
	try (HGraph graph = GraphFactory.createGraph()) {
	    long start = System.currentTimeMillis();
	    int number = 10000;
	    for (int i = 0; i < number; ++i) {
		Set<String> labels = new HashSet<>();
		Map<String, Object> properties = new HashMap<>();
		for (int j = 0; j < 10; ++j) {
		    labels.add("label" + j);
		    properties.put("property" + j, j);
		}
		Vertex vertex = graph.addVertex("TestId" + i, labels, properties);
		assertEquals("TestId" + i, vertex.getId().toString());
	    }
	    graph.commit();
	    long stop = System.currentTimeMillis();
	    long duration = stop - start;
	    double speed = (double) number / (double) duration * 1000.0;
	    System.out.println("time: " + duration + "ms");
	    System.out.println("speed: " + speed + " full_vertices/s");
	    System.out.println("speed: " + 1000 / speed + " ms/full_vertex");
	    assertTrue(duration < 10000);
	}
    }

    @Test
    public void testSetPropertyPerformance() throws IOException {
	try (HGraph graph = GraphFactory.createGraph()) {
	    Vertex vertex = graph.addVertex("PropertyPerformanceTest");
	    long start = System.currentTimeMillis();
	    int number = 10000;
	    for (int i = 0; i < number; ++i) {
		vertex.setProperty("key" + i, i);
		assertEquals(i, ((Integer) vertex.getProperty("key" + i)).intValue());
	    }
	    graph.commit();
	    long stop = System.currentTimeMillis();
	    long duration = stop - start;
	    double speed = (double) number / (double) duration * 1000.0;
	    System.out.println("time: " + duration + "ms");
	    System.out.println("speed: " + speed + " properties/s");
	    System.out.println("speed: " + 1000 / speed + " ms/property");
	    assertTrue(duration < 10000);
	}
    }

    @Test
    public void testSetLabelPerformance() throws IOException {
	try (HGraph graph = GraphFactory.createGraph()) {
	    HGraphVertex vertex = graph.addVertex("LabelPerformanceTest");
	    long start = System.currentTimeMillis();
	    int number = 10000;
	    for (int i = 0; i < number; ++i) {
		vertex.addLabel("label" + i);
		assertTrue(vertex.hasLabel("label" + i));
	    }
	    graph.commit();
	    long stop = System.currentTimeMillis();
	    long duration = stop - start;
	    double speed = (double) number / (double) duration * 1000.0;
	    System.out.println("time: " + duration + "ms");
	    System.out.println("speed: " + speed + " labels/s");
	    System.out.println("speed: " + 1000 / speed + " ms/label");
	    assertTrue(duration < 10000);
	}
    }

}
