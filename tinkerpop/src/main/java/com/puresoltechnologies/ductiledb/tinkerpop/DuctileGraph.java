package com.puresoltechnologies.ductiledb.tinkerpop;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedGraph;

import com.google.protobuf.ServiceException;
import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphFactory;
import com.puresoltechnologies.ductiledb.tinkerpop.compute.DuctileGraphComputerView;
import com.puresoltechnologies.ductiledb.tinkerpop.features.DuctileFeatures;
import com.puresoltechnologies.ductiledb.tinkerpop.gremlin.GremlinQueryExecutor;

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_PERFORMANCE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_PERFORMANCE)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT_PERFORMANCE)
@Graph.OptIn("com.puresoltechnologies.ductiledb.tinkerpop.test.StructureTestSuite")
public class DuctileGraph implements Graph, WrappedGraph<com.puresoltechnologies.ductiledb.api.DuctileDBGraph> {

    public static final String ZOOKEEPER_HOST_PROPERTY = DuctileDBGraphFactory.ZOOKEEPER_HOST_PROPERTY;
    public static final String ZOOKEEPER_PORT_PROPERTY = DuctileDBGraphFactory.ZOOKEEPER_PORT_PROPERTY;
    public static final String HBASE_MASTER_HOST_PROPERTY = DuctileDBGraphFactory.HBASE_MASTER_HOST_PROPERTY;
    public static final String HBASE_MASTER_PORT_PROPERTY = DuctileDBGraphFactory.HBASE_MASTER_PORT_PROPERTY;

    public static DuctileGraph open(Configuration configuration)
	    throws MasterNotRunningException, ZooKeeperConnectionException, ServiceException, IOException {
	String zookeeperHost = configuration.getString("zookeeper.host");
	int zookeeperPort = configuration.getInt("zookeeper.port");
	String masterHost = configuration.getString("hbase.master.host");
	int masterPort = configuration.getInt("hbase.master.port");
	new DuctileDBGraphFactory();
	DuctileDBGraph baseGraph = DuctileDBGraphFactory.createGraph(zookeeperHost, zookeeperPort, masterHost,
		masterPort);
	return new DuctileGraph(baseGraph, configuration);
    }

    private DuctileGraphComputerView graphComputerView = null;
    private final DuctileDBGraph baseGraph;
    private final BaseConfiguration configuration = new BaseConfiguration();
    private final DuctileTransaction ductileTransaction;
    private final DuctileFeatures features = new DuctileFeatures();

    protected DuctileGraph(DuctileDBGraph baseGraph, Configuration configuration) {
	this.configuration.copy(configuration);
	this.baseGraph = baseGraph;
	this.ductileTransaction = new DuctileTransaction(this);
    }

    protected DuctileGraph(File hbaseSiteFile, Configuration configuration) throws IOException, ServiceException {
	this.configuration.copy(configuration);
	this.baseGraph = DuctileDBGraphFactory.createGraph(hbaseSiteFile);
	this.ductileTransaction = new DuctileTransaction(this);
    }

    @Override
    public DuctileVertex addVertex(Object... keyValues) {
	ElementHelper.legalPropertyKeyValueArray(keyValues);
	if (ElementHelper.getIdValue(keyValues).isPresent())
	    throw Vertex.Exceptions.userSuppliedIdsNotSupported();
	tx().readWrite();
	DuctileDBVertex vertex = baseGraph.addVertex();
	String[] labels = ElementHelper.getLabelValue(keyValues).orElse("").split(DuctileVertex.LABEL_DELIMINATOR);
	for (String label : labels) {
	    if (!label.isEmpty()) {
		vertex.addType(label);
	    }
	}
	DuctileVertex ductileVertex = new DuctileVertex(vertex, this);
	ElementHelper.attachProperties(ductileVertex, keyValues);
	return ductileVertex;
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
	try {
	    return graphComputerClass.newInstance();
	} catch (InstantiationException | IllegalAccessException e) {
	    throw new IllegalArgumentException("Could not instantiate graph computer '" + graphComputerClass + "'.", e);
	}
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
	throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
	if (vertexIds.length > 0) {
	    List<Vertex> vertices = new ArrayList<>();
	    for (Object vertexId : vertexIds) {
		DuctileDBVertex baseVertex = baseGraph.getVertex(convertId(vertexId));
		if (baseVertex != null) {
		    vertices.add(new DuctileVertex(baseVertex, this));
		}
	    }
	    return vertices.iterator();
	} else {
	    return new VertexIterator(baseGraph.getVertices());
	}
    }

    private class VertexIterator implements Iterator<Vertex> {

	private final Iterator<DuctileDBVertex> iterator;

	public VertexIterator(Iterable<DuctileDBVertex> iterable) {
	    super();
	    this.iterator = iterable.iterator();
	}

	@Override
	public boolean hasNext() {
	    return iterator.hasNext();
	}

	@Override
	public Vertex next() {
	    return new DuctileVertex(iterator.next(), DuctileGraph.this);
	}
    }

    private long convertId(Object vertexId) {
	if (Long.class.isAssignableFrom(vertexId.getClass())) {
	    return (long) vertexId;
	} else if (Integer.class.equals(vertexId.getClass())) {
	    return ((Integer) vertexId).longValue();
	} else if (Double.class.equals(vertexId.getClass())) {
	    return Math.round((Double) vertexId);
	} else if (String.class.equals(vertexId.getClass())) {
	    return Long.parseLong((String) vertexId);
	} else {
	    throw new IllegalArgumentException(
		    "Edge id '" + vertexId + "' (class='" + vertexId.getClass() + "') is not supported.");
	}
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
	if (edgeIds.length > 0) {
	    List<Edge> edges = new ArrayList<>();
	    for (Object edgeId : edgeIds) {
		DuctileDBEdge baseEdge = baseGraph.getEdge(convertId(edgeId));
		edges.add(new DuctileEdge(baseEdge, this));
	    }
	    return edges.iterator();
	} else {
	    return new EdgeIterator(baseGraph.getEdges());
	}
    }

    private class EdgeIterator implements Iterator<Edge> {

	private final Iterator<DuctileDBEdge> iterator;

	public EdgeIterator(Iterable<DuctileDBEdge> iterable) {
	    super();
	    this.iterator = iterable.iterator();
	}

	@Override
	public boolean hasNext() {
	    return iterator.hasNext();
	}

	@Override
	public Edge next() {
	    return new DuctileEdge(iterator.next(), DuctileGraph.this);
	}
    }

    @Override
    public Transaction tx() {
	return ductileTransaction;
    }

    @Override
    public void close() throws IOException {
	tx().close();
	if (baseGraph != null) {
	    baseGraph.close();
	}
    }

    @Override
    public Variables variables() {
	throw Graph.Exceptions.variablesNotSupported();
    }

    @Override
    public Configuration configuration() {
	return configuration;
    }

    @Override
    public DuctileDBGraph getBaseGraph() {
	return baseGraph;
    }

    @Override
    public Features features() {
	return features;
    }

    public DuctileGraphComputerView createGraphComputerView(Set<String> computeKeys) {
	graphComputerView = new DuctileGraphComputerView(this, computeKeys);
	return graphComputerView;
    }

    public void dropGraphComputerView() {
	graphComputerView = null;
    }

    public DuctileGraphComputerView getGraphComputerView() {
	return graphComputerView;
    }

    public boolean inComputerMode() {
	return graphComputerView != null;
    }

    public GremlinQueryExecutor createGremlinQueryExecutor() {
	return new GremlinQueryExecutor(this);
    }
}
