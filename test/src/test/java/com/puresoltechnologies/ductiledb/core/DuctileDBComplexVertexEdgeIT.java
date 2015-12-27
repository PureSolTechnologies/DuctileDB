package com.puresoltechnologies.ductiledb.core;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.schema.DuctileDBHealthCheck;

public class DuctileDBComplexVertexEdgeIT extends AbstractDuctileDBGraphTest {

    private static DuctileDBGraphImpl graph;
    private static DuctileDBHealthCheck healthChecker;

    @BeforeClass
    public static void initializeHealthCheck() throws IOException {
	graph = getGraph();
	healthChecker = new DuctileDBHealthCheck(graph);
    }

    @Before
    public void cleanup() throws IOException {
	DuctileDBTestHelper.removeGraph(graph);
    }

    @After
    public void checkHealth() throws IOException {
	healthChecker.runCheck();
    }

    /**
     * Checks whether a vertex removal leads also to the complete removal of all
     * edges and their indizes.
     * 
     * @throws IOException
     */
    @Test
    public void testRemoveVertexWithEdges() throws IOException {
	DuctileDBVertex vertex1 = graph.addVertex();
	DuctileDBVertex vertex2 = graph.addVertex();
	DuctileDBVertex vertex3 = graph.addVertex();
	vertex1.addEdge("edge12", vertex2);
	vertex2.addEdge("edge23", vertex3);
	vertex3.addEdge("edge31", vertex1);
	graph.commit();
	assertEquals(3, DuctileDBTestHelper.count(graph.getEdges()));

	vertex2.remove();
	graph.commit();
	assertEquals(1, DuctileDBTestHelper.count(graph.getEdges()));
    }

    @Test
    public void testGraph() throws IOException {
	int num = 10;
	StandardGraphs.createGraph(graph, num);
	assertEquals(num, DuctileDBTestHelper.count(graph.getVertices()));
	assertEquals(num * (num - 1) / 2, DuctileDBTestHelper.count(graph.getEdges()));
	graph.commit();
	assertEquals(num, DuctileDBTestHelper.count(graph.getVertices()));
	assertEquals(num * (num - 1) / 2, DuctileDBTestHelper.count(graph.getEdges()));
	// 2nd try...
	num = 5;
	StandardGraphs.createGraph(graph, num);
	assertEquals(10 + 5, DuctileDBTestHelper.count(graph.getVertices()));
	assertEquals(45 + 10, DuctileDBTestHelper.count(graph.getEdges()));
	graph.commit();
    }

}
