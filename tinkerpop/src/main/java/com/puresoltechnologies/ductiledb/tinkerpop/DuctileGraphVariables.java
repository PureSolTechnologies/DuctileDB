package com.puresoltechnologies.ductiledb.tinkerpop;

import java.util.Optional;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Graph;

public class DuctileGraphVariables implements Graph.Variables {

    private final DuctileGraph ductileGraph;

    public DuctileGraphVariables(DuctileGraph ductileGraph) {
	this.ductileGraph = ductileGraph;
    }

    @Override
    public Set<String> keys() {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public <R> Optional<R> get(String key) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public void set(String key, Object value) {
	// TODO Auto-generated method stub
    }

    @Override
    public void remove(String key) {
	// TODO Auto-generated method stu
    }

}
