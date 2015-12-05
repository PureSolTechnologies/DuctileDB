package com.puresoltechnologies.ductiledb.tinkerpop;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;
import org.apache.tinkerpop.gremlin.structure.Graph;

import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.core.DuctileDBTestHelper;
import com.puresoltechnologies.ductiledb.core.GraphFactory;

public class DuctileGraphProvider extends AbstractGraphProvider {

    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {
	try (DuctileDBGraph ductileGraph = GraphFactory.createGraph(configuration)) {
	    DuctileDBTestHelper.removeGraph(ductileGraph);
	}
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Set<Class> getImplementations() {
	Set<Class> implementations = new HashSet<>();
	implementations.add(DuctileEdge.class);
	implementations.add(DuctileElement.class);
	implementations.add(DuctileGraph.class);
	implementations.add(DuctileGraphVariables.class);
	implementations.add(DuctileProperty.class);
	implementations.add(DuctileVertex.class);
	implementations.add(DuctileVertexProperty.class);
	return implementations;
    }

    @Override
    public Map<String, Object> getBaseConfiguration(String arg0, Class<?> arg1, String arg2, GraphData arg3) {
	HashMap<String, Object> baseConfiguration = new HashMap<>();
	baseConfiguration.put(Graph.GRAPH, DuctileGraph.class.getName());
	return baseConfiguration;
    }

}
