package com.puresoltechnologies.ductiledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;

import org.junit.BeforeClass;
import org.junit.Test;

import com.tinkerpop.blueprints.Vertex;

public class DuctileDBGraphImplIT extends AbstractDuctileDBTest {

    private static final int NUMBER = 10000;

    @BeforeClass
    public static void addTestData() throws IOException {
	try (DuctileDBGraph graph = GraphFactory.createGraph()) {
	    StarWarsGraph.addStarWarsFiguresData(graph);
	}
    }

    @Test
    public void testVertexIdCreator() throws IOException {
	try (DuctileDBGraphImpl graph = (DuctileDBGraphImpl) GraphFactory.createGraph()) {
	    long first = graph.createVertexId();
	    long second = graph.createVertexId();
	    long third = graph.createVertexId();
	    assertEquals(first + 1, second);
	    assertEquals(first + 2, third);
	}
    }

    @Test
    public void testVertexIdCreatorPerformance() throws IOException {
	try (DuctileDBGraphImpl graph = (DuctileDBGraphImpl) GraphFactory.createGraph()) {
	    long last = -1;
	    long start = System.currentTimeMillis();
	    for (int i = 0; i < NUMBER; ++i) {
		long current = graph.createVertexId();
		if (last >= 0) {
		    assertEquals(last + 1, current);
		}
		last = current;
	    }
	    long stop = System.currentTimeMillis();
	    long duration = stop - start;
	    double speed = (double) NUMBER / (double) duration * 1000.0;
	    System.out.println("time: " + duration + "ms");
	    System.out.println("speed: " + speed + " vertex ids/s");
	    System.out.println("speed: " + 1000 / speed + " ms/vertex id");
	    assertTrue(duration < 10000);
	}
    }

    @Test
    public void testEdgeIdCreator() throws IOException {
	try (DuctileDBGraphImpl graph = (DuctileDBGraphImpl) GraphFactory.createGraph()) {
	    long first = graph.createEdgeId();
	    long second = graph.createEdgeId();
	    long third = graph.createEdgeId();
	    assertEquals(first + 1, second);
	    assertEquals(first + 2, third);
	}
    }

    @Test
    public void testEdgeIdCreatorPerformance() throws IOException {
	try (DuctileDBGraphImpl graph = (DuctileDBGraphImpl) GraphFactory.createGraph()) {
	    long last = -1;
	    long start = System.currentTimeMillis();
	    for (int i = 0; i < NUMBER; ++i) {
		long current = graph.createEdgeId();
		if (last >= 0) {
		    assertEquals(last + 1, current);
		}
		last = current;
	    }
	    long stop = System.currentTimeMillis();
	    long duration = stop - start;
	    double speed = (double) NUMBER / (double) duration * 1000.0;
	    System.out.println("time: " + duration + "ms");
	    System.out.println("speed: " + speed + " edge ids/s");
	    System.out.println("speed: " + 1000 / speed + " ms/edge id");
	    assertTrue(duration < 10000);
	}
    }

    @Test
    public void testPropertySearch() throws IOException {
	try (DuctileDBGraph graph = GraphFactory.createGraph()) {
	    Iterable<Vertex> vertices = graph.getVertices("FirstName", "Luke");
	    Iterator<Vertex> iterator = vertices.iterator();
	    assertTrue(iterator.hasNext());
	    DuctileDBVertex vertex = (DuctileDBVertex) iterator.next();
	    assertEquals("Luke", vertex.getProperty("FirstName"));
	    assertEquals("Skywalker", vertex.getProperty("LastName"));
	    assertTrue(vertex.hasLabel("Yeti"));
	    assertFalse(iterator.hasNext());

	    graph.removeVertex(vertex);
	    graph.commit();

	    vertices = graph.getVertices("FirstName", "Luke");
	    iterator = vertices.iterator();
	    assertFalse(iterator.hasNext());
	}
    }

    @Test
    public void testLabelSearch() throws IOException {
	try (DuctileDBGraph graph = GraphFactory.createGraph()) {
	    Iterable<DuctileDBVertex> vertices = graph.getVertices("Yeti");
	    Iterator<DuctileDBVertex> iterator = vertices.iterator();
	    assertTrue(iterator.hasNext());
	    int count = 0;
	    while (iterator.hasNext()) {
		iterator.next();
		count++;
	    }
	    assertEquals(4, count);
	}
    }

}
