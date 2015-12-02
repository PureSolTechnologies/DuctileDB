package com.puresoltechnologies.ductiledb.core;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.puresoltechnologies.ductiledb.api.Edge;
import com.puresoltechnologies.ductiledb.api.Vertex;
import com.puresoltechnologies.ductiledb.core.AbstractDuctileDBGraphTest;

public class DuctileDBTransactionIT extends AbstractDuctileDBGraphTest {

    @Test
    public void testTransaction() {
	Vertex vertex1 = graph.addVertex();
	Vertex vertex2 = graph.addVertex();
	Edge edge = graph.addEdge(vertex1, vertex2, "edge");

	assertNull(graph.getVertex(vertex1.getId()));
	assertNull(graph.getVertex(vertex2.getId()));
	assertNull(graph.getEdge(edge.getId()));

	graph.commit();

	assertNotNull(graph.getVertex(vertex1.getId()));
	assertNotNull(graph.getVertex(vertex2.getId()));
	assertNotNull(graph.getEdge(edge.getId()));

	edge.remove();

	assertNotNull(graph.getVertex(vertex1.getId()));
	assertNotNull(graph.getVertex(vertex2.getId()));
	assertNotNull(graph.getEdge(edge.getId()));

	graph.commit();

	assertNotNull(graph.getVertex(vertex1.getId()));
	assertNotNull(graph.getVertex(vertex2.getId()));
	assertNull(graph.getEdge(edge.getId()));

	vertex1.remove();
	vertex2.remove();

	assertNotNull(graph.getVertex(vertex1.getId()));
	assertNotNull(graph.getVertex(vertex2.getId()));
	assertNull(graph.getEdge(edge.getId()));

	graph.commit();

	assertNull(graph.getVertex(vertex1.getId()));
	assertNull(graph.getVertex(vertex2.getId()));
	assertNull(graph.getEdge(edge.getId()));
    }

    @Test
    public void testRollback() {
	Vertex vertex1 = graph.addVertex();
	Vertex vertex2 = graph.addVertex();
	Edge edge = graph.addEdge(vertex1, vertex2, "edge");

	assertNull(graph.getVertex(vertex1.getId()));
	assertNull(graph.getVertex(vertex2.getId()));
	assertNull(graph.getEdge(edge.getId()));

	graph.rollback();

	assertNull(graph.getVertex(vertex1.getId()));
	assertNull(graph.getVertex(vertex2.getId()));
	assertNull(graph.getEdge(edge.getId()));

	graph.commit();

	assertNull(graph.getVertex(vertex1.getId()));
	assertNull(graph.getVertex(vertex2.getId()));
	assertNull(graph.getEdge(edge.getId()));
    }
}
