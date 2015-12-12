package com.puresoltechnologies.ductiledb.core.tx;

import static org.junit.Assert.assertEquals;
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

    @Test
    public void testCommitForSingleVertex() throws IOException {
	DuctileDBVertex vertex = graph.addVertex();

	assertInTransaction(vertex);
	assertNotInGraph(vertex);

	graph.commit();

	assertInTransaction(vertex);
	assertInGraph(vertex);

	vertex.addLabel("label");

	assertInTransaction(vertex);
	assertUnequalInGraph(vertex);

	graph.commit();

	assertInTransaction(vertex);
	assertInGraph(vertex);

	vertex.removeLabel("label");

	assertInTransaction(vertex);
	assertUnequalInGraph(vertex);

	graph.commit();

	assertInTransaction(vertex);
	assertInGraph(vertex);

	vertex.setProperty("key", "value");

	assertInTransaction(vertex);
	assertUnequalInGraph(vertex);

	graph.commit();

	assertInTransaction(vertex);
	assertInGraph(vertex);

	vertex.removeProperty("key");

	assertInTransaction(vertex);
	assertUnequalInGraph(vertex);

	graph.commit();

	assertInTransaction(vertex);
	assertInGraph(vertex);
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

	assertInTransaction(vertex1);
	assertInTransaction(vertex2);
	assertInTransaction(edge);

	assertNotInGraph(vertex1);
	assertNotInGraph(vertex2);
	assertNotInGraph(edge);

	graph.commit();

	assertInGraph(vertex1);
	assertInGraph(vertex2);
	assertInGraph(edge);

	edge.remove();

	assertInTransaction(vertex1);
	assertInTransaction(vertex2);
	assertNotInTransaction(edge);

	assertInGraphWithDifferentEdges(vertex1);
	assertInGraphWithDifferentEdges(vertex2);
	assertInGraph(edge);

	graph.commit();

	assertInGraph(vertex1);
	assertInGraph(vertex2);
	assertNotInGraph(edge);

	vertex1.remove();
	vertex2.remove();

	assertNotInTransaction(vertex1);
	assertNotInTransaction(vertex2);
	assertNotInTransaction(edge);

	assertInGraph(vertex1);
	assertInGraph(vertex2);
	assertNotInGraph(edge);

	graph.commit();

	assertNotInGraph(vertex1);
	assertNotInGraph(vertex2);
	assertNotInGraph(edge);
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

	assertInTransaction(vertex1);
	assertInTransaction(vertex2);
	assertInTransaction(edge);

	assertNotInGraph(vertex1);
	assertNotInGraph(vertex2);
	assertNotInGraph(edge);

	graph.rollback();

	assertNotInTransaction(vertex1);
	assertNotInTransaction(vertex2);
	assertNotInTransaction(edge);

	assertNotInGraph(vertex1);
	assertNotInGraph(vertex2);
	assertNotInGraph(edge);

	graph.commit();

	assertNotInTransaction(vertex1);
	assertNotInTransaction(vertex2);
	assertNotInTransaction(edge);

	assertNotInGraph(vertex1);
	assertNotInGraph(vertex2);
	assertNotInGraph(edge);
    }

    /**
     * This test checks the behavior of changes on not committed elements.
     * 
     * @throws IOException
     */
    @Test
    public void testChangesOnNotCommittedElements() throws IOException {
	DuctileDBVertex vertex1 = graph.addVertex();
	DuctileDBVertex vertex2 = graph.addVertex();
	DuctileDBEdge edge = graph.addEdge(vertex1, vertex2, "edge");

	assertInTransaction(vertex1);
	assertInTransaction(vertex2);
	assertInTransaction(edge);

	assertNotInGraph(vertex1);
	assertNotInGraph(vertex2);
	assertNotInGraph(edge);

	/*
	 * The next operation needs to work...
	 */
	edge.setProperty("property1", "value1");
	assertEquals("value1", edge.getProperty("property1"));

	assertUnequalInTransaction(vertex1);
	assertUnequalInTransaction(vertex2);
	assertInTransaction(edge);

	assertNotInGraph(vertex1);
	assertNotInGraph(vertex2);
	assertNotInGraph(edge);

	graph.commit();

	assertInGraph(vertex1);
	assertInGraph(vertex2);
	assertInGraph(edge);
	/*
	 * The next operation needs to work...
	 */
	edge.removeProperty("property1");
	assertNull(edge.getProperty("property1"));

	assertUnequalInTransaction(vertex1);
	assertUnequalInTransaction(vertex2);
	assertInTransaction(edge);

	assertUnequalInGraph(vertex1);
	assertUnequalInGraph(vertex2);
	assertUnequalInGraph(edge);

	graph.commit();

	assertInGraph(vertex1);
	assertInGraph(vertex2);
	assertInGraph(edge);
    }
}
