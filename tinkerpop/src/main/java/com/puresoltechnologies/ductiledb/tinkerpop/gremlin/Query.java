package com.puresoltechnologies.ductiledb.tinkerpop.gremlin;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.structure.Graph;

import com.puresoltechnologies.ductiledb.tinkerpop.DuctileGraph;

public class Query {

    private final DuctileGraph ductileGraph;

    public Query(DuctileGraph ductileGraph) {
	super();
	this.ductileGraph = ductileGraph;
    }

    public void query(String script) {
	final ExecutorService evalExecutor = Executors.newSingleThreadExecutor();
	final GremlinExecutor gremlinExecutor = GremlinExecutor.build().afterSuccess(b -> {
	    final Graph graph = (Graph) b.get("g");
	    if (graph.features().graph().supportsTransactions())
		graph.tx().commit();
	}).executorService(evalExecutor).create();

	Map<String, Object> bindings = new HashMap<>();
	bindings.put("g", ductileGraph);

	final AtomicInteger vertexCount = new AtomicInteger(0);

	final ExecutorService iterationExecutor = Executors.newSingleThreadExecutor();
	gremlinExecutor.eval("g.V().out()", bindings).thenAcceptAsync(o -> {
	    final Iterator itty = (Iterator) o;
	    itty.forEachRemaining(v -> vertexCount.incrementAndGet());
	} , iterationExecutor).join();

    }

}
