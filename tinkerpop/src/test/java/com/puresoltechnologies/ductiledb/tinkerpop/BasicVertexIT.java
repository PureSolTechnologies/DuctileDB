package com.puresoltechnologies.ductiledb.tinkerpop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

public class BasicVertexIT extends AbstractTinkerpopTest {

    @Test
    public void testCreateVertices() {
	Graph graph = getGraph();
	Vertex vertex1 = graph.addVertex("label", "vertex1");
	Vertex vertex2 = graph.addVertex("label", "vertex2", "key1", "value1", "key2", "value2");
	graph.tx().commit();

	Iterator<Vertex> vertices = graph.vertices(vertex1.id(), vertex2.id());
	assertTrue(vertices.hasNext());
	Vertex readVertex1 = vertices.next();
	assertTrue(vertices.hasNext());
	Vertex readVertex2 = vertices.next();
	assertFalse(vertices.hasNext());
	assertEquals(vertex1, readVertex1);
	assertEquals(vertex2, readVertex2);
    }

    @Test
    public void testCreateVerticesWithEdge() {
	Graph graph = getGraph();
	Vertex vertex1 = graph.addVertex("label", "vertex1");
	Vertex vertex2 = graph.addVertex("label", "vertex2", "key1", "value1", "key2", "value2");
	vertex1.addEdge("edge1", vertex2);

	Iterator<Vertex> vertices = graph.vertices(vertex1.id(), vertex2.id());
	assertFalse(vertices.hasNext());

	graph.tx().commit();

	vertices = graph.vertices(vertex1.id(), vertex2.id());
	assertTrue(vertices.hasNext());
	Vertex readVertex1 = vertices.next();
	assertTrue(vertices.hasNext());
	Vertex readVertex2 = vertices.next();
	assertFalse(vertices.hasNext());
	assertEquals(vertex1, readVertex1);
	assertEquals(vertex2, readVertex2);
    }

}
