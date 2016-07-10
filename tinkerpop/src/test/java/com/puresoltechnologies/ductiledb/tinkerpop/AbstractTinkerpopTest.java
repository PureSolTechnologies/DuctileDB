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

import com.puresoltechnologies.ductiledb.core.graph.DuctileDBGraphFactory;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBTestHelper;
import com.puresoltechnologies.ductiledb.core.graph.schema.DuctileDBHealthCheck;

public abstract class AbstractTinkerpopTest {

    private static DuctileGraph graph;

    @BeforeClass
    public static void connect() throws IOException {
	// DuctileDBTestHelper.removeTables();
	Map<String, String> configuration = createDefaultConfiguration();
	graph = (DuctileGraph) GraphFactory.open(configuration);
	assertNotNull("Graph was not opened.", graph);
    }

    /**
     * This method creates the default configuration for localhost and default
     * ports.
     * 
     * @return A {@link Map} with the configuration is returned.
     */
    public static Map<String, String> createDefaultConfiguration() {
	Map<String, String> configuration = new HashMap<>();
	configuration.put(Graph.GRAPH, DuctileGraph.class.getName());
	configuration.put(DuctileGraph.ZOOKEEPER_HOST_PROPERTY, "localhost");
	configuration.put(DuctileGraph.ZOOKEEPER_PORT_PROPERTY,
		String.valueOf(DuctileDBGraphFactory.DEFAULT_ZOOKEEPER_PORT));
	configuration.put(DuctileGraph.HBASE_MASTER_HOST_PROPERTY, "localhost");
	configuration.put(DuctileGraph.HBASE_MASTER_PORT_PROPERTY,
		String.valueOf(DuctileDBGraphFactory.DEFAULT_MASTER_PORT));
	return configuration;
    }

    @AfterClass
    public static void disconnect() throws Exception {
	graph.close();
    }

    @Before
    public final void cleanup() throws IOException {
	DuctileDBTestHelper.removeGraph(graph.getBaseGraph());
	DuctileDBHealthCheck.runCheckForEmpty((DuctileDBGraphImpl) graph.getBaseGraph());
    }

    public static DuctileGraph getGraph() {
	return graph;
    }
}
