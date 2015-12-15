package com.puresoltechnologies.ductiledb.tinkerpop.features;

import org.apache.tinkerpop.gremlin.structure.Graph.Features.VariableFeatures;

public class DuctileVariableFeatures extends DuctileDataTypeFeatures implements VariableFeatures {

    @Override
    public boolean supportsVariables() {
	return false;
    }

}
