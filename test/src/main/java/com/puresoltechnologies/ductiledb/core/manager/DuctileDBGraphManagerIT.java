package com.puresoltechnologies.ductiledb.core.manager;

import static org.junit.Assert.assertEquals;

import org.junit.BeforeClass;
import org.junit.Test;

import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.manager.DuctileDBGraphManager;
import com.puresoltechnologies.ductiledb.core.AbstractDuctileDBGraphTest;

public class DuctileDBGraphManagerIT extends AbstractDuctileDBGraphTest {

    private static DuctileDBGraph graph;

    @BeforeClass
    public static void initialize() {
	graph = getGraph();
    }

    @Test
    public void testVersion() {
	DuctileDBGraphManager graphManager = graph.getGraphManager();
	assertEquals("0.1.0", graphManager.getVersion().toString());
    }
}
