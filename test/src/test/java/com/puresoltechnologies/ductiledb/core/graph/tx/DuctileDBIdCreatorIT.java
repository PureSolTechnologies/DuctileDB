package com.puresoltechnologies.ductiledb.core.graph.tx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.puresoltechnologies.ductiledb.core.graph.AbstractDuctileDBGraphTest;
import com.puresoltechnologies.ductiledb.core.graph.GraphStoreImpl;
import com.puresoltechnologies.ductiledb.core.graph.StarWarsGraph;
import com.puresoltechnologies.ductiledb.core.graph.tx.DuctileDBTransactionImpl;

/**
 * This integration tests check the creation of vertex and edge IDs.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBIdCreatorIT extends AbstractDuctileDBGraphTest {

    private static final int NUMBER = 10000;

    private static GraphStoreImpl graph;
    private static GraphStoreImpl graphImpl;

    @BeforeClass
    public static void initialize() throws IOException {
	graph = getGraph();
	graphImpl = (graph);
	StarWarsGraph.addStarWarsFiguresData(graphImpl);
    }

    @Test
    public void testVertexIdCreator() throws IOException {
	DuctileDBTransactionImpl currentTransaction = (DuctileDBTransactionImpl) graphImpl.createTransaction();
	long first = currentTransaction.createVertexId();
	long second = currentTransaction.createVertexId();
	long third = currentTransaction.createVertexId();
	assertEquals(first + 1, second);
	assertEquals(first + 2, third);
    }

    @Test
    public void testVertexIdCreatorPerformance() throws IOException {
	DuctileDBTransactionImpl currentTransaction = (DuctileDBTransactionImpl) graphImpl.createTransaction();
	long last = -1;
	long start = System.currentTimeMillis();
	for (int i = 0; i < NUMBER; ++i) {
	    long current = currentTransaction.createVertexId();
	    if (last >= 0) {
		assertEquals(last + 1, current);
	    }
	    last = current;
	}
	long stop = System.currentTimeMillis();
	long duration = stop - start;
	double speed = (double) NUMBER / (double) duration * 1000.0;
	System.out.println("time: " + duration + "ms");
	System.out.println("speed: " + speed + " vertex ids/s");
	System.out.println("speed: " + 1000 / speed + " ms/vertex id");
	assertTrue(duration < 10000);
    }

    @Test
    public void testEdgeIdCreator() throws IOException {
	DuctileDBTransactionImpl currentTransaction = (DuctileDBTransactionImpl) graphImpl.createTransaction();
	long first = currentTransaction.createEdgeId();
	long second = currentTransaction.createEdgeId();
	long third = currentTransaction.createEdgeId();
	assertEquals(first + 1, second);
	assertEquals(first + 2, third);
    }

    @Test
    public void testEdgeIdCreatorPerformance() throws IOException {
	DuctileDBTransactionImpl currentTransaction = (DuctileDBTransactionImpl) graphImpl.createTransaction();
	long last = -1;
	long start = System.currentTimeMillis();
	for (int i = 0; i < NUMBER; ++i) {
	    long current = currentTransaction.createEdgeId();
	    if (last >= 0) {
		assertEquals(last + 1, current);
	    }
	    last = current;
	}
	long stop = System.currentTimeMillis();
	long duration = stop - start;
	double speed = (double) NUMBER / (double) duration * 1000.0;
	System.out.println("time: " + duration + "ms");
	System.out.println("speed: " + speed + " edge ids/s");
	System.out.println("speed: " + 1000 / speed + " ms/edge id");
	assertTrue(duration < 10000);
    }
}
