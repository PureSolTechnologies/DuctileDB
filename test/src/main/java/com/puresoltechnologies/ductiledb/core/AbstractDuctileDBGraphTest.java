package com.puresoltechnologies.ductiledb.core;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.apache.commons.configuration.BaseConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;

public class AbstractDuctileDBGraphTest {

    protected static DuctileDBGraph graph;

    @BeforeClass
    public static void connect() throws IOException {
	graph = GraphFactory.createGraph(new BaseConfiguration());
	assertNotNull("Graph was not created.", graph);
	DuctileDBTestHelper.removeGraph(graph);
    }

    @AfterClass
    public static void disconnect() throws IOException {
	graph.close();
	graph = null;
    }
}
