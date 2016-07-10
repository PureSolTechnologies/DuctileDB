package com.puresoltechnologies.ductiledb.tinkerpop;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphFactory;

public class DuctileGraphProvider extends AbstractGraphProvider {

    private static final Logger logger = LoggerFactory.getLogger(DuctileGraphProvider.class);

    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {
	if (graph != null) {
	    DuctileDBGraph ductileGraph = ((DuctileGraph) graph).getBaseGraph();
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
    public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test,
	    final String testMethodName, final LoadGraphWith.GraphData loadGraphWith) {
	return getBaseConfiguration();
    }

    public Map<String, Object> getBaseConfiguration() {
	HashMap<String, Object> baseConfiguration = new HashMap<>();
	baseConfiguration.put(Graph.GRAPH, DuctileGraph.class.getName());
	baseConfiguration.put(DuctileGraph.ZOOKEEPER_HOST_PROPERTY, "localhost");
	baseConfiguration.put(DuctileGraph.ZOOKEEPER_PORT_PROPERTY,
		String.valueOf(DuctileDBGraphFactory.DEFAULT_ZOOKEEPER_PORT));
	baseConfiguration.put(DuctileGraph.HBASE_MASTER_HOST_PROPERTY, "localhost");
	baseConfiguration.put(DuctileGraph.HBASE_MASTER_PORT_PROPERTY,
		String.valueOf(DuctileDBGraphFactory.DEFAULT_MASTER_PORT));
	return baseConfiguration;
    }

}
