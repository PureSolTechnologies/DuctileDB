package com.puresoltechnologies.ductiledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Connection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class GraphFactoryIT extends AbstractDuctileDBTest {

    private static DuctileDBGraph graph;

    @BeforeClass
    public static void connect() throws IOException {
	graph = GraphFactory.createGraph();
    }

    @AfterClass
    public static void disconnect() throws IOException {
	graph.close();
    }

    @Test
    public void testConnection() throws IOException {
	assertNotNull(graph);
	assertEquals(DuctileDBGraphImpl.class, graph.getClass());
	Connection connection = graph.getConnection();
	assertNotNull(connection);
    }

}
