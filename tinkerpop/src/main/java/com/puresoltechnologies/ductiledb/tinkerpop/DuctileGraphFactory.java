package com.puresoltechnologies.ductiledb.tinkerpop;

import org.apache.commons.configuration.Configuration;

import com.puresoltechnologies.ductiledb.core.graph.GraphStore;

/**
 * This class is used to create a new {@link DuctileGraph} object.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class DuctileGraphFactory {

    public static DuctileGraph createGraph(GraphStore ductileDB, Configuration configuration) {
	return new DuctileGraph(ductileDB, configuration);
    }

}
