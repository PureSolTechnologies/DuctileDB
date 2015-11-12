package com.puresoltechnologies.ductiledb;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.apache.commons.configuration.BaseConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class AbstractDuctileDBGraphTest extends AbstractDuctileDBTest {

    protected static DuctileDBGraph graph;

    @BeforeClass
    public static void connect() throws IOException {
	graph = GraphFactory.createGraph(new BaseConfiguration());
	assertNotNull("Graph was not created.", graph);
    }

    @AfterClass
    public static void disconnect() throws IOException {
	graph.close();
	graph = null;
    }
}
