package com.puresoltechnologies.ductiledb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.api.Direction;
import com.puresoltechnologies.ductiledb.api.Edge;
import com.puresoltechnologies.ductiledb.api.Vertex;
import com.puresoltechnologies.ductiledb.core.AbstractDuctileDBGraphTest;

public class DuctileDBEdgeIT extends AbstractDuctileDBGraphTest {

    @Test
    public void testAddAndRemoveEdge() {
	Vertex vertex1 = graph.addVertex();
	Vertex vertex2 = graph.addVertex();
	Edge edge = vertex1.addEdge("TestEdge", vertex2);
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

	Edge readEdge = graph.getEdge(edge.getId());
	assertEquals(edge, readEdge);

	readEdge.remove();
	graph.commit();

	readEdge = graph.getEdge(edge.getId());
	assertNull(readEdge);
    }
}
