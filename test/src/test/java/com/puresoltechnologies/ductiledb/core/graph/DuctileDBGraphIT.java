package com.puresoltechnologies.ductiledb.core.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;

import org.junit.BeforeClass;
import org.junit.Test;

import com.puresoltechnologies.ductiledb.core.graph.GraphStoreImpl;
import com.puresoltechnologies.ductiledb.core.graph.manager.DuctileDBGraphManager;
import com.puresoltechnologies.ductiledb.core.graph.manager.DuctileDBGraphManagerImpl;

public class DuctileDBGraphIT extends AbstractDuctileDBGraphTest {

    private static GraphStoreImpl graph;

    @BeforeClass
    public static void initialize() throws IOException {
	graph = getGraph();
    }

    @Test
    public void testGraphReturnsGraphManager() {
	DuctileDBGraphManager graphManager = graph.createGraphManager();
	assertNotNull("No graph manager provided.", graphManager);
	assertEquals("The wrong implementation of graph manager is provided.", DuctileDBGraphManagerImpl.class,
		graphManager.getClass());
    }

    @Test
    public void testPropertySearch() throws IOException {
	StarWarsGraph.addStarWarsFiguresData(graph);
	Iterable<DuctileDBVertex> vertices = graph.getVertices(StarWarsGraph.FIRST_NAME_PROPERTY, "Luke");
	Iterator<DuctileDBVertex> iterator = vertices.iterator();
	assertTrue(iterator.hasNext());
	DuctileDBVertex vertex = iterator.next();
	assertEquals("Luke", vertex.getProperty(StarWarsGraph.FIRST_NAME_PROPERTY));
	assertEquals("Skywalker", vertex.getProperty(StarWarsGraph.LAST_NAME_PROPERTY));
	assertTrue(vertex.hasType("Yeti"));
	assertFalse(iterator.hasNext());
    }

    @Test
    public void testTypeSearch() throws IOException {
	StarWarsGraph.addStarWarsFiguresData(graph);
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
    public void testEdge() throws IOException {
	StarWarsGraph.addStarWarsFiguresData(graph);
	Iterator<DuctileDBVertex> lukes = graph.getVertices(StarWarsGraph.FIRST_NAME_PROPERTY, "Luke").iterator();
	assertTrue(lukes.hasNext());
	DuctileDBVertex lukeSkywalker = lukes.next();
	assertEquals("Luke", lukeSkywalker.getProperty(StarWarsGraph.FIRST_NAME_PROPERTY));
	assertEquals("Skywalker", lukeSkywalker.getProperty(StarWarsGraph.LAST_NAME_PROPERTY));
	assertFalse(lukes.hasNext());
	Iterator<DuctileDBEdge> edges = lukeSkywalker.getEdges(EdgeDirection.OUT, StarWarsGraph.HAS_SISTER_EDGE)
		.iterator();
	assertTrue(edges.hasNext());
	DuctileDBEdge hasSister = edges.next();
	DuctileDBVertex leiaOrgana = hasSister.getTargetVertex();
	assertEquals("Leia", leiaOrgana.getProperty(StarWarsGraph.FIRST_NAME_PROPERTY));
	assertFalse(edges.hasNext());
    }

    @Test
    public void testGetVertices() throws IOException {
	StarWarsGraph.addStarWarsFiguresData(graph);
	Iterable<DuctileDBVertex> vertices = graph.getVertices();
	int count = 0;
	for (@SuppressWarnings("unused")
	DuctileDBVertex vertex : vertices) {
	    count++;
	}
	assertEquals(10, count);
    }

    @Test
    public void testGetEdges() throws IOException {
	StarWarsGraph.addStarWarsFiguresData(graph);
	Iterable<DuctileDBEdge> edges = graph.getEdges();
	int count = 0;
	for (@SuppressWarnings("unused")
	DuctileDBEdge edge : edges) {
	    count++;
	}
	assertEquals(3, count);
    }

    @Test
    public void testStreamDelete() throws IOException {
	assertEquals(0, DuctileDBTestHelper.count(graph.getVertices()));
	assertEquals(0, DuctileDBTestHelper.count(graph.getEdges()));

	StandardGraphs.createGraph(graph, 5);
	assertEquals(5, DuctileDBTestHelper.count(graph.getVertices()));
	assertEquals(10, DuctileDBTestHelper.count(graph.getEdges()));
	graph.commit();

	assertEquals(5, DuctileDBTestHelper.count(graph.getVertices()));
	assertEquals(10, DuctileDBTestHelper.count(graph.getEdges()));
	graph.rollback();

	graph.getEdges().forEach(DuctileDBEdge::remove);
	assertEquals(5, DuctileDBTestHelper.count(graph.getVertices()));
	assertEquals(0, DuctileDBTestHelper.count(graph.getEdges()));
	graph.commit();

	assertEquals(5, DuctileDBTestHelper.count(graph.getVertices()));
	assertEquals(0, DuctileDBTestHelper.count(graph.getEdges()));
	graph.rollback();

	graph.getVertices().forEach(DuctileDBVertex::remove);
	assertEquals(0, DuctileDBTestHelper.count(graph.getVertices()));
	assertEquals(0, DuctileDBTestHelper.count(graph.getEdges()));
	graph.commit();

	assertEquals(0, DuctileDBTestHelper.count(graph.getVertices()));
	assertEquals(0, DuctileDBTestHelper.count(graph.getEdges()));
	graph.rollback();
    }
}
