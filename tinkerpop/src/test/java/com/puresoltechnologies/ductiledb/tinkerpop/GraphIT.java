package com.puresoltechnologies.ductiledb.tinkerpop;

import static org.junit.Assert.assertTrue;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.Test;

public class GraphIT extends AbstractTinkerpopTest {

    @Test
    public void testForCorrectImplementation() {
	Graph graph = getGraph();
	assertTrue("The provided implementation is not the expected implementation for DuctileDB!",
		DuctileGraph.class.isAssignableFrom(graph.getClass()));
    }

}
