package com.puresoltechnologies.ductiledb.tinkerpop.gremlin;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import com.puresoltechnologies.ductiledb.tinkerpop.DuctileGraph;

public class GremlinQueryExecutor {

    private final GremlinExecutor gremlinExecutor;
    private final Map<String, Object> bindings = new HashMap<>();
    private final BasicThreadFactory threadFactory = new BasicThreadFactory.Builder()
	    .namingPattern("gremlin-executor-%d").build();

    public GremlinQueryExecutor(DuctileGraph ductileGraph) {
	super();
	ExecutorService evalExecutor = Executors.newSingleThreadExecutor(threadFactory);
	gremlinExecutor = GremlinExecutor.build().afterSuccess(b -> {
	    final GraphTraversalSource graph = (GraphTraversalSource) b.get("g");
	    graph.tx().commit();
	}).afterFailure((b, e) -> {
	    final GraphTraversalSource graph = (GraphTraversalSource) b.get("g");
	    graph.tx().rollback();
	}).scriptEvaluationTimeout(60000).executorService(evalExecutor).create();

	bindings.put("g", ductileGraph.traversal());
    }

    public List<Object> query(String queryScript) {
	List<Object> results = new LinkedList<>();
	final ExecutorService executor = Executors.newSingleThreadExecutor(threadFactory);
	gremlinExecutor.eval(queryScript, bindings).thenAcceptAsync(i -> {
	    final Iterator<?> itty = (Iterator<?>) i;
	    itty.forEachRemaining(e -> results.add(e));
	} , executor).join();
	return results;
    }
}
