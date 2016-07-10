package com.puresoltechnologies.ductiledb.tinkerpop;

import java.io.Serializable;
import java.util.Optional;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Graph;

import com.puresoltechnologies.ductiledb.api.graph.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.graph.manager.DuctileDBGraphManager;

public class DuctileGraphVariables implements Graph.Variables {

    private final DuctileGraph ductileGraph;

    public DuctileGraphVariables(DuctileGraph ductileGraph) {
	this.ductileGraph = ductileGraph;
    }

    @Override
    public Set<String> keys() {
	DuctileDBGraph baseGraph = ductileGraph.getBaseGraph();
	DuctileDBGraphManager graphManager = baseGraph.createGraphManager();
	return (Set<String>) graphManager.getVariableNames();
    }

    @Override
    public <R> Optional<R> get(String key) {
	DuctileDBGraph baseGraph = ductileGraph.getBaseGraph();
	DuctileDBGraphManager graphManager = baseGraph.createGraphManager();
	return graphManager.getVariable(key);
    }

    @Override
    public void set(String key, Object value) {
	DuctileDBGraph baseGraph = ductileGraph.getBaseGraph();
	DuctileDBGraphManager graphManager = baseGraph.createGraphManager();
	graphManager.setVariable(key, (Serializable) value);
    }

    @Override
    public void remove(String key) {
	DuctileDBGraph baseGraph = ductileGraph.getBaseGraph();
	DuctileDBGraphManager graphManager = baseGraph.createGraphManager();
	graphManager.removeVariable(key);
    }

}
