package com.puresoltechnologies.ductiledb.core.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import org.junit.BeforeClass;
import org.junit.Test;

import com.puresoltechnologies.ductiledb.core.graph.GraphStoreImpl;

public class DuctileDBEdgeIT extends AbstractDuctileDBGraphTest {

    private static GraphStoreImpl graph;

    @BeforeClass
    public static void initialize() {
	graph = getGraph();
    }

    @Test
    public void testAddAndRemoveEdge() throws IOException {
	DuctileDBVertex vertex1 = graph.addVertex();
	DuctileDBVertex vertex2 = graph.addVertex();
	DuctileDBEdge edge = vertex1.addEdge("TestEdge", vertex2, Collections.emptyMap());
	assertEquals(vertex1, edge.getStartVertex());
	assertEquals(vertex2, edge.getTargetVertex());

	graph.commit();

	Iterator<DuctileDBEdge> edges = vertex1.getEdges(EdgeDirection.OUT, "TestEdge").iterator();
	assertTrue(edges.hasNext());
	assertNotNull(edges.next());
	assertFalse(edges.hasNext());

	edges = vertex2.getEdges(EdgeDirection.IN, "TestEdge").iterator();
	assertTrue(edges.hasNext());
	assertNotNull(edges.next());
	assertFalse(edges.hasNext());

	DuctileDBEdge readEdge = graph.getEdge(edge.getId());
	assertEquals(edge, readEdge);

	readEdge.remove();
	graph.commit();

	assertNotInTransaction(edge);
    }
}
