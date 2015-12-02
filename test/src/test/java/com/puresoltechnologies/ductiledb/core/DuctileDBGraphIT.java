package com.puresoltechnologies.ductiledb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;

import org.junit.BeforeClass;
import org.junit.Test;

import com.puresoltechnologies.ductiledb.api.EdgeDirection;
import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.AbstractDuctileDBGraphTest;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.core.StarWarsGraph;

public class DuctileDBGraphIT extends AbstractDuctileDBGraphTest {

    private static final int NUMBER = 10000;

    private static DuctileDBGraphImpl graphImpl;

    @BeforeClass
    public static void initialize() {
	graphImpl = ((DuctileDBGraphImpl) graph);
	StarWarsGraph.addStarWarsFiguresData(graphImpl);
    }

    @Test
    public void testVertexIdCreator() throws IOException {
	long first = graphImpl.createVertexId();
	long second = graphImpl.createVertexId();
	long third = graphImpl.createVertexId();
	assertEquals(first + 1, second);
	assertEquals(first + 2, third);
    }

    @Test
    public void testVertexIdCreatorPerformance() throws IOException {
	long last = -1;
	long start = System.currentTimeMillis();
	for (int i = 0; i < NUMBER; ++i) {
	    long current = graphImpl.createVertexId();
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

    @Test
    public void testEdgeIdCreator() throws IOException {
	long first = graphImpl.createEdgeId();
	long second = graphImpl.createEdgeId();
	long third = graphImpl.createEdgeId();
	assertEquals(first + 1, second);
	assertEquals(first + 2, third);
    }

    @Test
    public void testEdgeIdCreatorPerformance() throws IOException {
	long last = -1;
	long start = System.currentTimeMillis();
	for (int i = 0; i < NUMBER; ++i) {
	    long current = graphImpl.createEdgeId();
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

    @Test
    public void testPropertySearch() throws IOException {
	Iterable<DuctileDBVertex> vertices = graph.getVertices(StarWarsGraph.FIRST_NAME_PROPERTY, "Luke");
	Iterator<DuctileDBVertex> iterator = vertices.iterator();
	assertTrue(iterator.hasNext());
	DuctileDBVertex vertex = iterator.next();
	assertEquals("Luke", vertex.getProperty(StarWarsGraph.FIRST_NAME_PROPERTY));
	assertEquals("Skywalker", vertex.getProperty(StarWarsGraph.LAST_NAME_PROPERTY));
	assertTrue(vertex.hasLabel("Yeti"));
	assertFalse(iterator.hasNext());
    }

    @Test
    public void testLabelSearch() throws IOException {
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

    @Test
    public void testEdge() {
	Iterator<DuctileDBVertex> lukes = graph.getVertices(StarWarsGraph.FIRST_NAME_PROPERTY, "Luke").iterator();
	assertTrue(lukes.hasNext());
	DuctileDBVertex lukeSkywalker = lukes.next();
	assertEquals("Luke", lukeSkywalker.getProperty(StarWarsGraph.FIRST_NAME_PROPERTY));
	assertEquals("Skywalker", lukeSkywalker.getProperty(StarWarsGraph.LAST_NAME_PROPERTY));
	assertFalse(lukes.hasNext());
	Iterator<DuctileDBEdge> edges = lukeSkywalker.getEdges(EdgeDirection.OUT, StarWarsGraph.HAS_SISTER_EDGE).iterator();
	assertTrue(edges.hasNext());
	DuctileDBEdge hasSister = edges.next();
	DuctileDBVertex leiaOrgana = hasSister.getVertex(EdgeDirection.IN);
	assertEquals("Leia", leiaOrgana.getProperty(StarWarsGraph.FIRST_NAME_PROPERTY));
	assertFalse(edges.hasNext());
    }

    @Test
    public void testGetVertices() {
	Iterable<DuctileDBVertex> vertices = graph.getVertices();
	int count = 0;
	for (@SuppressWarnings("unused")
	DuctileDBVertex vertex : vertices) {
	    count++;
	}
	assertEquals(10, count);
    }

    @Test
    public void testGetEdges() {
	Iterable<DuctileDBEdge> edges = graph.getEdges();
	int count = 0;
	for (@SuppressWarnings("unused")
	DuctileDBEdge edge : edges) {
	    count++;
	}
	assertEquals(3, count);
    }
}
