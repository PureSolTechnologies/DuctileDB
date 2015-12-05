package com.puresoltechnologies.ductiledb.tinkerpop.features;

import org.apache.tinkerpop.gremlin.structure.Graph.Features.EdgeFeatures;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.EdgePropertyFeatures;

public class DuctileEdgeFeatures extends DuctileElementFeatures implements EdgeFeatures {

    private final DuctileEdgePropertyFeatures ductileEdgePropertyFeatures = new DuctileEdgePropertyFeatures();

    @Override
    public boolean supportsAddEdges() {
	return true;
    }

    @Override
    public boolean supportsRemoveEdges() {
	return true;
    }

    @Override
    public EdgePropertyFeatures properties() {
	return ductileEdgePropertyFeatures;
    }

}
