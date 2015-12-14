package com.puresoltechnologies.ductiledb.tinkerpop.features;

import org.apache.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VariableFeatures;

public class DuctileGraphFeatures implements GraphFeatures {

    private final DuctileVariableFeatures ductileVariableFeatures = new DuctileVariableFeatures();

    @Override
    public boolean supportsComputer() {
	return false;
    }

    @Override
    public boolean supportsPersistence() {
	return true;
    }

    @Override
    public boolean supportsConcurrentAccess() {
	return true;
    }

    @Override
    public boolean supportsTransactions() {
	return true;
    }

    @Override
    public boolean supportsThreadedTransactions() {
	return true;
    }

    @Override
    public VariableFeatures variables() {
	return ductileVariableFeatures;
    }

}
