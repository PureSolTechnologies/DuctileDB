package com.puresoltechnologies.ductiledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import com.tinkerpop.blueprints.Vertex;

public class DuctileDBGraphImplIT extends AbstractDuctileDBTest {

    @Before
    public void addTestData() throws IOException {
	try (DuctileDBGraph graph = GraphFactory.createGraph()) {
	    StarWarsGraph.addStarWarsFiguresData(graph);
	}
    }

    @Test
    public void testPropertySearch() throws IOException {
	try (DuctileDBGraph graph = GraphFactory.createGraph()) {
	    DuctileDBVertex vertex = graph.getVertex("Luke Skywalker");
	    assertEquals("Luke", vertex.getProperty("FirstName"));
	    assertEquals("Skywalker", vertex.getProperty("LastName"));
	    assertTrue(vertex.hasLabel("Yeti"));

	    Iterable<Vertex> vertices = graph.getVertices("FirstName", "Luke");
	    Iterator<Vertex> iterator = vertices.iterator();
	    assertTrue(iterator.hasNext());
	    vertex = (DuctileDBVertex) iterator.next();
	    assertEquals("Luke", vertex.getProperty("FirstName"));
	    assertEquals("Skywalker", vertex.getProperty("LastName"));
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
