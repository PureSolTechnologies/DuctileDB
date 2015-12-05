package com.puresoltechnologies.ductiledb.tinkerpop.features;

import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;

public class DuctileVertexFeatures extends DuctileElementFeatures implements VertexFeatures {

    private final DuctileVertexPropertyFeatures ductileVertexPropertyFeatures = new DuctileVertexPropertyFeatures();

    @Override
    public Cardinality getCardinality(final String key) {
	return Cardinality.single;
    }

    @Override
    public boolean supportsAddVertices() {
	return true;
    }

    @Override
    public boolean supportsRemoveVertices() {
	return true;
    }

    @Override
    public boolean supportsMultiProperties() {
	return false;
    }

    @Override
    public boolean supportsMetaProperties() {
	return false;
    }

    @Override
    public VertexPropertyFeatures properties() {
	return ductileVertexPropertyFeatures;
    }

}
