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

import com.puresoltechnologies.ductiledb.core.AbstractDuctileDBTest;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBTestHelper;
import com.puresoltechnologies.ductiledb.core.graph.schema.DuctileDBHealthCheck;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;

public abstract class AbstractTinkerpopTest extends AbstractDuctileDBTest {

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
	configuration.put(DuctileGraph.DUCTILEDB_CONFIG_FILE_PROPERTY,
		AbstractDuctileDBTest.DEFAULT_TEST_CONFIG_URL.toString());
	return configuration;
    }

    @AfterClass
    public static void disconnect() throws Exception {
	graph.close();
    }

    @Before
    public final void cleanup() throws IOException, StorageException {
	DuctileDBTestHelper.removeGraph(graph.getBaseGraph());
	DuctileDBHealthCheck.runCheckForEmpty((DuctileDBGraphImpl) graph.getBaseGraph());
    }

    public static DuctileGraph getGraph() {
	return graph;
    }
}
