package com.puresoltechnologies.ductiledb.tinkerpop.features;

import org.apache.tinkerpop.gremlin.structure.Graph.Features;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public class DuctileFeatures implements Features {

    protected GraphFeatures graphFeatures = new DuctileGraphFeatures();
    protected VertexFeatures vertexFeatures = new DuctileVertexFeatures();
    protected EdgeFeatures edgeFeatures = new DuctileEdgeFeatures();

    @Override
    public GraphFeatures graph() {
	return graphFeatures;
    }

    @Override
    public VertexFeatures vertex() {
	return vertexFeatures;
    }

    @Override
    public EdgeFeatures edge() {
	return edgeFeatures;
    }

    @Override
    public String toString() {
	return StringFactory.featureString(this);
    }

}
