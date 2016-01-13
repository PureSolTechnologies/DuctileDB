package com.puresoltechnologies.ductiledb.tinkerpop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import com.puresoltechnologies.ductiledb.core.DuctileDBTestHelper;

public class DuctileVertexIT extends AbstractTinkerpopTest {

    @Test
    public void testCreateSingleVertex() {
	Graph graph = getGraph();
	Vertex vertex = graph.addVertex("label", "vertex1");

	Iterator<Vertex> vertices = graph.vertices(vertex.id());
	assertNotNull(vertices);
	assertTrue(vertices.hasNext());
	assertEquals(vertex, vertices.next());
	assertFalse(vertices.hasNext());

	graph.tx().commit();

	vertices = graph.vertices(vertex.id());
	assertNotNull(vertices);
	assertTrue(vertices.hasNext());
	assertEquals(vertex, vertices.next());
	assertFalse(vertices.hasNext());
    }

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
	assertTrue(vertices.hasNext());

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

    @Test
    public void testVertexCRUD() {
	Graph graph = getGraph();
	assertEquals(0, DuctileDBTestHelper.count(graph.vertices()));

	Vertex vertex = graph.addVertex("label", "vertex", "key", "value");
	graph.tx().commit();

	assertEquals(1, DuctileDBTestHelper.count(graph.vertices()));
	Vertex readVertex = graph.vertices().next();
	assertEquals(vertex, readVertex);

	vertex.property("key2", "value2");
	graph.tx().commit();

	assertEquals("value2", vertex.property("key2").value());
	assertEquals(1, DuctileDBTestHelper.count(graph.vertices()));
	readVertex = graph.vertices().next();
	assertEquals(vertex, readVertex);

	vertex.remove();
	graph.tx().commit();

	assertEquals(0, DuctileDBTestHelper.count(graph.vertices()));
    }

    @Test
    public void testPropertyCRUD() {
	Graph graph = getGraph();
	assertEquals(0, DuctileDBTestHelper.count(graph.vertices()));

	Vertex vertex = graph.addVertex();
	graph.tx().commit();
	assertEquals(1, DuctileDBTestHelper.count(graph.vertices()));
	Vertex readVertex = graph.vertices().next();
	assertEquals(vertex, readVertex);

	vertex.property("key", "value");
	assertEquals("value", vertex.property("key").value());
	graph.tx().commit();
	readVertex = graph.vertices().next();
	assertEquals("value", readVertex.property("key").value());

	vertex.property("key", "value2");
	assertEquals("value2", vertex.property("key").value());
	graph.tx().commit();
	readVertex = graph.vertices().next();
	assertEquals("value2", readVertex.property("key").value());

	vertex.property("key").remove();
	graph.tx().commit();
	readVertex = graph.vertices().next();
	assertFalse(readVertex.property("key").isPresent());
    }
}
