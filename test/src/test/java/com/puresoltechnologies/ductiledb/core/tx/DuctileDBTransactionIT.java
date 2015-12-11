package com.puresoltechnologies.ductiledb.core.tx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.AbstractDuctileDBGraphTest;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.core.DuctileDBHealthCheck;
import com.puresoltechnologies.ductiledb.core.DuctileDBTestHelper;

/**
 * This intergrationt tests check the correct behavior of transactions.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBTransactionIT extends AbstractDuctileDBGraphTest {

    @Before
    public void cleanupGraph() throws IOException {
	DuctileDBTestHelper.removeGraph(graph);
    }

    @After
    public void checkGraph() throws IOException {
	DuctileDBHealthCheck.runCheck((DuctileDBGraphImpl) graph);
    }

    /**
     * Checks the simple creation of vertices and edges and the correct behavior
     * before and after commits.
     * 
     * @throws IOException
     */
    @Test
    public void testBasicCommit() throws IOException {
	DuctileDBVertex vertex1 = graph.addVertex();
	DuctileDBVertex vertex2 = graph.addVertex();
	DuctileDBEdge edge = graph.addEdge(vertex1, vertex2, "edge");

	assertEquals(vertex1, graph.getVertex(vertex1.getId()));
	assertEquals(vertex2, graph.getVertex(vertex2.getId()));
	assertEquals(edge, graph.getEdge(edge.getId()));

	graph.commit();

	assertNotNull(graph.getVertex(vertex1.getId()));
	assertNotNull(graph.getVertex(vertex2.getId()));
	assertNotNull(graph.getEdge(edge.getId()));

	edge.remove();

	assertEquals(vertex1, graph.getVertex(vertex1.getId()));
	assertEquals(vertex2, graph.getVertex(vertex2.getId()));
	assertNull(graph.getEdge(edge.getId()));

	graph.commit();

	assertEquals(vertex1, graph.getVertex(vertex1.getId()));
	assertEquals(vertex2, graph.getVertex(vertex2.getId()));
	assertNull(graph.getEdge(edge.getId()));

	vertex1.remove();
	vertex2.remove();

	assertNull(graph.getVertex(vertex1.getId()));
	assertNull(graph.getVertex(vertex2.getId()));
	assertNull(graph.getEdge(edge.getId()));

	graph.commit();

	assertNull(graph.getVertex(vertex1.getId()));
	assertNull(graph.getVertex(vertex2.getId()));
	assertNull(graph.getEdge(edge.getId()));
    }

    /**
     * Checks that a rollback really drops the changes in the transaction and a
     * following commit will not change the graph.
     * 
     * @throws IOException
     */
    @Test
    public void testRollback() throws IOException {
	DuctileDBVertex vertex1 = graph.addVertex();
	DuctileDBVertex vertex2 = graph.addVertex();
	DuctileDBEdge edge = graph.addEdge(vertex1, vertex2, "edge");

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

    /**
     * This test checks the behavior of changes on not committed elements.
     * 
     * @throws IOException
     */
    @Test
    public void testChangesOnNotCommittedElements() throws IOException {
	DuctileDBHealthCheck.runCheck((DuctileDBGraphImpl) graph);
	/*
	 * Create two vertices and an edge between them.
	 */
	DuctileDBVertex vertex1 = graph.addVertex();
	DuctileDBVertex vertex2 = graph.addVertex();
	DuctileDBEdge edge = graph.addEdge(vertex1, vertex2, "edge");
	/*
	 * Without a commit the vertices and edge are not available, yet.
	 */
	assertEquals(vertex1, graph.getVertex(vertex1.getId()));
	assertEquals(vertex2, graph.getVertex(vertex2.getId()));
	assertEquals(edge, graph.getEdge(edge.getId()));
	/*
	 * The next operation needs to work...
	 */
	edge.setProperty("property1", "value1");
	assertEquals("value1", edge.getProperty("property1"));
	/*
	 * Without a commit the vertices and edge are not available, yet.
	 */
	assertEquals(vertex1, graph.getVertex(vertex1.getId()));
	assertEquals(vertex2, graph.getVertex(vertex2.getId()));
	assertEquals(edge, graph.getEdge(edge.getId()));
	/*
	 * Commit...
	 */
	graph.commit();
	DuctileDBHealthCheck.runCheck((DuctileDBGraphImpl) graph);
	/*
	 * Vertices and edge are available now...
	 */
	assertNotNull(graph.getVertex(vertex1.getId()));
	assertNotNull(graph.getVertex(vertex2.getId()));
	DuctileDBEdge edge2 = graph.getEdge(edge.getId());
	assertNotNull(edge2);
	assertEquals("value1", edge2.getProperty("property1"));
	/*
	 * The next operation needs to work...
	 */
	edge.removeProperty("property1");
	assertNull(edge.getProperty("property1"));

	assertNotNull(graph.getVertex(vertex1.getId()));
	assertNotNull(graph.getVertex(vertex2.getId()));
	DuctileDBEdge edge3 = graph.getEdge(edge.getId());
	assertNotNull(edge3);
	assertEquals("value1", edge3.getProperty("property1"));

	graph.commit();

	DuctileDBHealthCheck.runCheck((DuctileDBGraphImpl) graph);
	DuctileDBEdge edge4 = graph.getEdge(edge.getId());
	assertNotNull(edge4);
	assertNull(edge4.getProperty("property1"));
    }
}
