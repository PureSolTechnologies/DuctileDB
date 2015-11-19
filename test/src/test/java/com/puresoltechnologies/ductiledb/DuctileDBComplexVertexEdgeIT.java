package com.puresoltechnologies.ductiledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import com.puresoltechnologies.commons.types.IntrospectionUtilities;

public class DuctileDBComplexVertexEdgeIT extends AbstractDuctileDBGraphTest {

    private static DuctileDBHealthCheck healthChecker;

    @BeforeClass
    public static void initializeHealthCheck() throws IOException {
	healthChecker = new DuctileDBHealthCheck(graph.getConnection());
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
	healthChecker.runCheck();

	vertex2.remove();
	graph.commit();
	assertEquals(1, DuctileDBTestHelper.count(graph.getEdges()));
	healthChecker.runCheck();
    }

    /**
     * Tests lazy loading of vertices after edge loading to not have a complete
     * eager graph loading.
     * 
     * @throws IOException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws NoSuchFieldException
     * @throws SecurityException
     */
    @Test
    public void testLazyVertexLoadingInEdges() throws IOException, SecurityException, NoSuchFieldException,
	    IllegalArgumentException, IllegalAccessException {
	DuctileDBVertex vertex1 = graph.addVertex();
	DuctileDBVertex vertex2 = graph.addVertex();
	DuctileDBEdge edge = vertex1.addEdge("edge12", vertex2);
	graph.commit();
	assertEquals(1, DuctileDBTestHelper.count(graph.getEdges()));
	healthChecker.runCheck();

	DuctileDBEdge readEdge = graph.getEdge(edge.getId());
	long startVertexId = (long) IntrospectionUtilities.getField(readEdge, "startVertexId");
	assertEquals((long) vertex1.getId(), startVertexId);
	long targetVertexId = (long) IntrospectionUtilities.getField(readEdge, "targetVertexId");
	assertEquals((long) vertex2.getId(), targetVertexId);
	DuctileDBVertex startVertex = (DuctileDBVertex) IntrospectionUtilities.getField(readEdge, "startVertex");
	assertNull(startVertex);
	DuctileDBVertex targetVertex = (DuctileDBVertex) IntrospectionUtilities.getField(readEdge, "targetVertex");
	assertNull(targetVertex);
	assertNotNull(readEdge.getStartVertex());
	assertNotNull(readEdge.getTargetVertex());

	vertex1.remove();
	vertex2.remove();
	graph.commit();
	healthChecker.runCheck();
    }
}