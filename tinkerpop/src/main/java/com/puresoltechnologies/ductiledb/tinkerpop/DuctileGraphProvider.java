package com.puresoltechnologies.ductiledb.tinkerpop;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;

public class DuctileGraphProvider extends AbstractGraphProvider {

    private static final Logger logger = LoggerFactory.getLogger(DuctileGraphProvider.class);

    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {
	try (DuctileDBGraph ductileGraph = ((DuctileGraph) graph).getBaseGraph()) {
	    logger.info("Delete ductile graph...");
	    for (DuctileDBEdge edge : ductileGraph.getEdges()) {
		edge.remove();
	    }
	    for (DuctileDBVertex vertex : ductileGraph.getVertices()) {
		vertex.remove();
	    }
	    ductileGraph.commit();
	    logger.info("Ductile graph deleted.");
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
    public Map<String, Object> getBaseConfiguration(String arg0, Class<?> arg1, String arg2, GraphData graphData) {
	return getBaseConfiguration();
    }

    public Map<String, Object> getBaseConfiguration() {
	HashMap<String, Object> baseConfiguration = new HashMap<>();
	baseConfiguration.put(Graph.GRAPH, DuctileGraph.class.getName());
	return baseConfiguration;
    }

}
