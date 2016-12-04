package com.puresoltechnologies.ductiledb.tinkerpop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.puresoltechnologies.ductiledb.core.graph.StandardGraphs;
import com.puresoltechnologies.ductiledb.tinkerpop.gremlin.GremlinQueryExecutor;

public class GremlinQueryExecutorIT extends AbstractTinkerpopTest {

    private static DuctileGraph graph;

    @BeforeClass
    public static void initialized() {
	graph = getGraph();
    }

    @Test
    public void test() throws IOException {
	StandardGraphs.createGraph(graph.getBaseGraph(), 5);
	graph.tx().commit();
	GremlinQueryExecutor gremlinQueryExecutor = graph.createGremlinQueryExecutor();
	List<Object> result = gremlinQueryExecutor.query("g.V()");
	assertNotNull(result);
	int counter = 0;
	for (Object o : result) {
	    System.out.println(o.getClass().getName() + ": " + ((DuctileVertex) o).getBaseElement().toString());
	    counter++;
	}
	assertEquals(5, counter);
    }

}
