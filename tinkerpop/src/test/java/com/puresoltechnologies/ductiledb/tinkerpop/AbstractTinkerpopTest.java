package com.puresoltechnologies.ductiledb.tinkerpop;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.google.protobuf.ServiceException;
import com.puresoltechnologies.ductiledb.core.AbstractDuctileDBGraphTest;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.core.DuctileDBTestHelper;
import com.puresoltechnologies.ductiledb.core.schema.DuctileDBHealthCheck;

public abstract class AbstractTinkerpopTest {

    private static DuctileGraph graph;

    @BeforeClass
    public static void connect() throws IOException {
	// DuctileDBTestHelper.removeTables();
	Map<String, String> configuration = new HashMap<>();
	configuration.put(Graph.GRAPH, DuctileGraph.class.getName());
	graph = (DuctileGraph) GraphFactory.open(configuration);
	assertNotNull("Graph was not opened.", graph);
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

    public static DuctileGraph createGraph()
	    throws MasterNotRunningException, ZooKeeperConnectionException, ServiceException, IOException {
	return DuctileGraphFactory.createGraph(AbstractDuctileDBGraphTest.DEFAULT_ZOOKEEPER_HOST,
		AbstractDuctileDBGraphTest.DEFAULT_ZOOKEEPER_PORT, AbstractDuctileDBGraphTest.DEFAULT_MASTER_HOST,
		AbstractDuctileDBGraphTest.DEFAULT_MASTER_PORT, new BaseConfiguration());
    }

    public static DuctileGraph getGraph() {
	return graph;
    }
}
