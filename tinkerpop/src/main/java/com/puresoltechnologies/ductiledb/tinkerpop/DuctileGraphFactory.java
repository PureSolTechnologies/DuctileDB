package com.puresoltechnologies.ductiledb.tinkerpop;

import java.io.IOException;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;

import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphFactory;

/**
 * This class is used to create a new {@link DuctileGraph} object.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class DuctileGraphFactory {

    /**
     * This method creates a new {@link DuctileGraph} based on an existing HBase
     * {@link Connection}.
     * 
     * @param connection
     * @param configuration
     * @return
     * @throws IOException
     */
    public static DuctileGraph createGraph(Connection connection, BaseConfiguration configuration) throws IOException {
	DuctileDBGraph ductileDBGraph = DuctileDBGraphFactory.createGraph(connection);
	return new DuctileGraph(ductileDBGraph, configuration);
    }

    /**
     * This method create a new {@link DuctileGraph} only based on its
     * configuration. A connection to HBase is opened during the process.
     * 
     * @param configuration
     * @return
     * @throws IOException
     */
    public static DuctileGraph createGraph(BaseConfiguration configuration) throws IOException {
	DuctileDBGraph graph = DuctileDBGraphFactory.createGraph(configuration);
	return new DuctileGraph(graph, configuration);
    }

}
