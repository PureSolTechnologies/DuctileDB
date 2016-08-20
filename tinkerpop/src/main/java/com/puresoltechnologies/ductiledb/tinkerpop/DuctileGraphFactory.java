package com.puresoltechnologies.ductiledb.tinkerpop;

import org.apache.commons.configuration.Configuration;

import com.puresoltechnologies.ductiledb.api.graph.DuctileDBGraph;

/**
 * This class is used to create a new {@link DuctileGraph} object.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class DuctileGraphFactory {

    public static DuctileGraph createGraph(DuctileDBGraph graph, Configuration configuration) {
	return new DuctileGraph(graph, configuration);
    }

}
