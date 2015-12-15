package com.puresoltechnologies.ductiledb.tinkerpop.features;

import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures;

public class DuctileVertexPropertyFeatures extends DuctilePropertyFeatures implements VertexPropertyFeatures {

    @Override
    public boolean supportsAddProperty() {
	return true;
    }

    @Override
    public boolean supportsRemoveProperty() {
	return true;
    }

    @Override
    public boolean supportsUserSuppliedIds() {
	return false;
    }

    @Override
    public boolean supportsNumericIds() {
	return true;
    }

    @Override
    public boolean supportsStringIds() {
	return false;
    }

    @Override
    public boolean supportsUuidIds() {
	return false;
    }

    @Override
    public boolean supportsCustomIds() {
	return false;
    }

    @Override
    public boolean supportsAnyIds() {
	return false;
    }

}
