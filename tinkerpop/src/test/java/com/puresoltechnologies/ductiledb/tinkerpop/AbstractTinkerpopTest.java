package com.puresoltechnologies.ductiledb.tinkerpop;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.puresoltechnologies.ductiledb.core.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.core.DuctileDBTestHelper;
import com.puresoltechnologies.ductiledb.core.schema.DuctileDBHealthCheck;

public abstract class AbstractTinkerpopTest {

    private static Graph graph;

    @BeforeClass
    public static void connect() throws IOException {
	Map<String, String> configuration = new HashMap<>();
	configuration.put(Graph.GRAPH, DuctileGraph.class.getName());
	graph = GraphFactory.open(configuration);
	assertNotNull("Graph was not opened.", graph);
    }

    @AfterClass
    public static void disconnect() throws Exception {
	graph.close();
    }

    @Before
    public void initialize() throws IOException {
	DuctileDBTestHelper.removeGraph(((DuctileGraph) graph).getBaseGraph());
	DuctileDBHealthCheck.runCheckForEmpty((DuctileDBGraphImpl) ((DuctileGraph) graph).getBaseGraph());
    }

    public static Graph getGraph() {
	return graph;
    }
}
