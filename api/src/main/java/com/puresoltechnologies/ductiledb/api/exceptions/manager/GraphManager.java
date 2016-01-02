package com.puresoltechnologies.ductiledb.api.exceptions.manager;

import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;

/**
 * The graph manager is used to configure the graph and define its schema.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface GraphManager {

    /**
     * This method returns the graph for which this manager is responsible for.
     * 
     * @return A {@link DuctileDBGraph} is returned.
     */
    public DuctileDBGraph getGraph();
}
