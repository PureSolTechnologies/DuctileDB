package com.puresoltechnologies.ductiledb.tinkerpop;

import static org.junit.Assert.assertNotNull;

import javax.script.ScriptException;

import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor.Builder;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.Test;

public class DuctileQueryIT extends AbstractTinkerpopTest {

    @Test
    public void testQuery() throws ScriptException {
	Graph graph = getGraph();
	graph.addVertex("label", "vertex1");
	graph.tx().commit();

	Builder executor = GremlinExecutor.build();
	GremlinExecutor gremlinExecutor = executor.create();
	DefaultGraphTraversal<?, ?> returnValue = (DefaultGraphTraversal<?, ?>) gremlinExecutor.compile("V()").get()
		.eval();
	returnValue.setGraph(graph);
	assertNotNull(returnValue);
	assertNotNull(returnValue.next());
    }

}
