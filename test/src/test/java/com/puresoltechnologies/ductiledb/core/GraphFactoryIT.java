package com.puresoltechnologies.ductiledb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

public class GraphFactoryIT extends AbstractDuctileDBGraphTest {

    private static DuctileDBGraphImpl graph;

    @BeforeClass
    public void initialize() {
	graph = getGraph();
    }

    @Test
    public void testConnection() throws IOException {
	assertNotNull(graph);
	assertEquals(DuctileDBGraphImpl.class, graph.getClass());
    }

}
