package com.puresoltechnologies.ductiledb.core.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.puresoltechnologies.ductiledb.core.graph.DuctileDBGraphImpl;

public class GraphFactoryIT extends AbstractDuctileDBGraphTest {

    private static DuctileDBGraphImpl graph;

    @BeforeClass
    public static void initialize() {
	graph = getGraph();
    }

    @Test
    public void testConnection() throws IOException {
	assertNotNull(graph);
	assertEquals(DuctileDBGraphImpl.class, graph.getClass());
    }

}
