package com.puresoltechnologies.ductiledb.tinkerpop.gremlin;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.structure.Graph;

import com.puresoltechnologies.ductiledb.tinkerpop.DuctileGraph;

public class GremlinQueryExecutor {

    private final GremlinExecutor gremlinExecutor;
    private final Map<String, Object> bindings = new HashMap<>();

    public GremlinQueryExecutor(DuctileGraph ductileGraph) {
	super();
	ExecutorService evalExecutor = Executors.newSingleThreadExecutor();
	gremlinExecutor = GremlinExecutor.build().afterSuccess(b -> {
	    final Graph graph = (Graph) b.get("g");
	    graph.tx().commit();
	}).afterFailure((b, e) -> {
	    final Graph graph = (Graph) b.get("g");
	    graph.tx().rollback();
	}).executorService(evalExecutor).create();

	bindings.put("g", ductileGraph.traversal());
    }

    public List<Object> query(String queryScript) {
	List<Object> results = new LinkedList<>();
	Executor executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
	gremlinExecutor.eval(queryScript, bindings).thenAcceptAsync(i -> {
	    final Iterator<?> itty = (Iterator<?>) i;
	    itty.forEachRemaining(e -> results.add(e));
	} , executor).join();
	return results;
    }
}
