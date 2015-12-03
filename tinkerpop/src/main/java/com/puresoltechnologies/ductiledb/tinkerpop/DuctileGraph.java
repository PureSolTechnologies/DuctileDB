package com.puresoltechnologies.ductiledb.tinkerpop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedGraph;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.GraphFactory;

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_PERFORMANCE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_PERFORMANCE)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT_PERFORMANCE)
public final class DuctileGraph implements Graph, WrappedGraph<com.puresoltechnologies.ductiledb.api.DuctileDBGraph> {

    public static DuctileGraph open(Configuration configuration) throws IOException {
	return new DuctileGraph(configuration);
    }

    private final DuctileDBGraph baseGraph;
    private final BaseConfiguration configuration = new BaseConfiguration();
    private DuctileGraphVariables ductileGraphVariables = new DuctileGraphVariables(this);

    protected DuctileGraph(DuctileDBGraph baseGraph, Configuration configuration) {
	this.baseGraph = baseGraph;
	this.configuration.copy(configuration);
    }

    protected DuctileGraph(Configuration configuration) throws IOException {
	this.configuration.copy(configuration);
	this.baseGraph = GraphFactory.createGraph(configuration);
    }

    @Override
    public DuctileVertex addVertex(Object... keyValues) {
	ElementHelper.legalPropertyKeyValueArray(keyValues);
	if (ElementHelper.getIdValue(keyValues).isPresent())
	    throw Vertex.Exceptions.userSuppliedIdsNotSupported();
	tx().readWrite();
	DuctileDBVertex vertex = baseGraph.addVertex();
	String[] labels = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL)
		.split(DuctileVertex.LABEL_DELIMINATOR);
	for (String label : labels) {
	    vertex.addLabel(label);
	}
	DuctileVertex ductileVertex = new DuctileVertex(vertex, this);
	ElementHelper.attachProperties(ductileVertex, keyValues);
	return ductileVertex;
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
	List<Vertex> vertices = new ArrayList<>();
	for (Object vertexId : vertexIds) {
	    DuctileDBVertex baseVertex = baseGraph.getVertex((Long) vertexId);
	    vertices.add(new DuctileVertex(baseVertex, this));
	}
	return vertices.iterator();
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
	List<Edge> edges = new ArrayList<>();
	for (Object vertexId : edgeIds) {
	    DuctileDBEdge baseEdge = baseGraph.getEdge(vertexId);
	    edges.add(new DuctileEdge(baseEdge, this));
	}
	return edges.iterator();
    }

    @Override
    public Transaction tx() {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public void close() throws Exception {
	tx().close();
	if (baseGraph != null) {
	    baseGraph.close();
	}
    }

    @Override
    public Variables variables() {
	return ductileGraphVariables;
    }

    @Override
    public Configuration configuration() {
	return configuration;
    }

    @Override
    public DuctileDBGraph getBaseGraph() {
	return baseGraph;
    }

}
