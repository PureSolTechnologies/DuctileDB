package com.puresoltechnologies.ductiledb.tinkerpop;

import java.io.File;
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

import com.puresoltechnologies.ductiledb.core.AbstractDuctileDBTest;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBGraphConfiguration;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class DuctileGraphProvider extends AbstractGraphProvider {

    private static final Logger logger = LoggerFactory.getLogger(DuctileGraphProvider.class);

    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {
	if (graph != null) {
	    logger.info("Delete ductile graph...");
	    DuctileDBGraphImpl ductileGraph = (DuctileDBGraphImpl) ((DuctileGraph) graph).getBaseGraph();
	    // DuctileDBTestHelper.removeGraph(ductileGraph);
	    DatabaseEngineImpl storageEngine = ductileGraph.getStorageEngine();
	    graph.close();
	    DuctileDBGraphConfiguration ductileDBConfiguration = ductileGraph.getConfiguration();
	    Storage storage = storageEngine.getStorage();
	    File graphDirectory = new File(ductileDBConfiguration.getNamespace());
	    if (storage.exists(graphDirectory)) {
		storage.removeDirectory(graphDirectory, true);
	    }
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
	HashMap<String, Object> baseConfiguration = new HashMap<>();
	baseConfiguration.put(Graph.GRAPH, DuctileGraph.class.getName());
	baseConfiguration.put(DuctileGraph.DUCTILEDB_CONFIG_FILE_PROPERTY,
		AbstractDuctileDBTest.DEFAULT_TEST_CONFIG_URL.toString());
	baseConfiguration.put(DuctileGraph.DUCTILEDB_NAMESPACE_PROPERTY, graphName);
	return baseConfiguration;
    }

}
