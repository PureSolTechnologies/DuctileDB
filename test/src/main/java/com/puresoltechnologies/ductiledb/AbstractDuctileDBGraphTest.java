package com.puresoltechnologies.ductiledb;

import java.io.IOException;

import org.apache.commons.configuration.BaseConfiguration;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class AbstractDuctileDBGraphTest extends AbstractDuctileDBTest {

    protected static DuctileDBGraph graph;
    protected static DuctileDBHealthCheck check;

    @BeforeClass
    public static void connect() throws IOException {
	graph = GraphFactory.createGraph(new BaseConfiguration());
	check = new DuctileDBHealthCheck(graph.getConnection());
    }

    @AfterClass
    public static void disconnect() throws IOException {
	check.runCheck();
	check = null;
	graph.close();
	graph = null;
    }

    @Before
    public void checkConsistency() throws IOException {
	check.runCheck();
    }
}
