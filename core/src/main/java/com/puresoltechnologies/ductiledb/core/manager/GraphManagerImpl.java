package com.puresoltechnologies.ductiledb.core.manager;

import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.exceptions.manager.GraphManager;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphImpl;

public class GraphManagerImpl implements GraphManager {

    private final DuctileDBGraphImpl ductileDBGraphImpl;

    public GraphManagerImpl(DuctileDBGraphImpl ductileDBGraphImpl) {
	super();
	this.ductileDBGraphImpl = ductileDBGraphImpl;
    }

    @Override
    public DuctileDBGraph getGraph() {
	return ductileDBGraphImpl;
    }
}
