package com.puresoltechnologies.ductiledb.tinkerpop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedGraph;

import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
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
@Graph.OptIn("com.puresoltechnologies.ductiledb.tinkerpop.NativeDuctileDBSuite")
public final class DuctileGraph
	implements Graph, WrappedGraph<com.puresoltechnologies.ductiledb.api.DuctileDBGraph> {

    private final DuctileDBGraph baseGraph;
    private final Configuration configuration;

    protected DuctileGraph(DuctileDBGraph baseGraph, Configuration configuration) {
	this.baseGraph = baseGraph;
	this.configuration = configuration;
    }

    protected DuctileGraph(Configuration configuration) throws IOException {
	this.configuration = configuration;
	this.baseGraph = GraphFactory.createGraph(configuration);
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
	// TODO Auto-generated method stub
	return null;
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
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public Transaction tx() {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public void close() throws Exception {
	// TODO Auto-generated method stub

    }

    @Override
    public Variables variables() {
	// TODO Auto-generated method stub
	return null;
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
