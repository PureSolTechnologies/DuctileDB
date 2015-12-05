package com.puresoltechnologies.ductiledb.tinkerpop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.puresoltechnologies.ductiledb.core.DuctileDBTestHelper;

public class BasicTinkerpopIT {

    private static Graph graph;

    @BeforeClass
    public static void connect() throws IOException {
	Map<String, String> configuration = new HashMap<>();
	configuration.put(Graph.GRAPH, DuctileGraph.class.getName());
	graph = GraphFactory.open(configuration);
	assertNotNull("Graph was not opened.", graph);
    }

    @AfterClass
    public static void disconnect() throws Exception {
	graph.close();
    }

    @Before
    public void initialize() throws IOException {
	DuctileDBTestHelper.removeGraph(((DuctileGraph) graph).getBaseGraph());
    }

    @Test
    public void testForCorrectImplementation() {
	assertTrue("The provided implementation is not the expected implementation for DuctileDB!",
		DuctileGraph.class.isAssignableFrom(graph.getClass()));
    }

    @Test
    public void testCreateVertices() {
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
	Vertex vertex1 = graph.addVertex("label", "vertex1");
	Vertex vertex2 = graph.addVertex("label", "vertex2", "key1", "value1", "key2", "value2");
	// vertex1.addEdge("edge1", vertex2);
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

}
