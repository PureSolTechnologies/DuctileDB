package com.puresoltechnologies.ductiledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Test;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;

public class DuctileDBEdgeIT extends AbstractDuctileDBGraphTest {

    @Test
    public void testAddAndRemoveEdge() {
	DuctileDBVertex vertex1 = graph.addVertex();
	DuctileDBVertex vertex2 = graph.addVertex();
	DuctileDBEdge edge = vertex1.addEdge("TestEdge", vertex2);
	assertEquals(vertex1, edge.getStartVertex());
	assertEquals(vertex2, edge.getTargetVertex());
	graph.commit();

	Iterator<Edge> edges = vertex1.getEdges(Direction.OUT, "TestEdge").iterator();
	assertTrue(edges.hasNext());
	assertNotNull(edges.next());
	assertFalse(edges.hasNext());

	edges = vertex2.getEdges(Direction.IN, "TestEdge").iterator();
	assertTrue(edges.hasNext());
	assertNotNull(edges.next());
	assertFalse(edges.hasNext());

	DuctileDBEdge readEdge = graph.getEdge(edge.getId());
	assertEquals(edge, readEdge);

	readEdge.remove();
	graph.commit();

	readEdge = graph.getEdge(edge.getId());
	assertNull(readEdge);
    }
}
