package com.puresoltechnologies.ductiledb.tinkerpop;

import java.io.Serializable;
import java.util.Optional;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Graph;

import com.puresoltechnologies.ductiledb.core.graph.GraphStore;
import com.puresoltechnologies.ductiledb.core.graph.manager.DuctileDBGraphManager;

public class DuctileGraphVariables implements Graph.Variables {

    private final DuctileGraph ductileGraph;

    public DuctileGraphVariables(DuctileGraph ductileGraph) {
	this.ductileGraph = ductileGraph;
    }

    @Override
    public Set<String> keys() {
	GraphStore baseGraph = ductileGraph.getBaseGraph();
	DuctileDBGraphManager graphManager = baseGraph.createGraphManager();
	return (Set<String>) graphManager.getVariableNames();
    }

    @Override
    public <R> Optional<R> get(String key) {
	GraphStore baseGraph = ductileGraph.getBaseGraph();
	DuctileDBGraphManager graphManager = baseGraph.createGraphManager();
	return graphManager.getVariable(key);
    }

    @Override
    public void set(String key, Object value) {
	GraphStore baseGraph = ductileGraph.getBaseGraph();
	DuctileDBGraphManager graphManager = baseGraph.createGraphManager();
	graphManager.setVariable(key, (Serializable) value);
    }

    @Override
    public void remove(String key) {
	GraphStore baseGraph = ductileGraph.getBaseGraph();
	DuctileDBGraphManager graphManager = baseGraph.createGraphManager();
	graphManager.removeVariable(key);
    }

}
