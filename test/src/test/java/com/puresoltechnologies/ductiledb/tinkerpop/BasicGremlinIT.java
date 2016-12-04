package com.puresoltechnologies.ductiledb.tinkerpop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import javax.script.ScriptException;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

public class BasicGremlinIT extends AbstractTinkerpopTest {

    @Test
    public void test() throws IOException, ScriptException {
	DuctileGraph graph = getGraph();
	assertNotNull(graph);
	assertEquals(DuctileGraph.class.getName(), graph.configuration().getString(Graph.GRAPH));
	Vertex vertex1 = graph.addVertex("label", "vertexLabel1");
	Vertex vertex2 = graph.addVertex("label", "vertexLabel2");
	vertex1.addEdge("edgeLabel", vertex2);
	graph.tx().commit();

	Iterator<Vertex> vertices = graph.vertices();
	assertTrue(vertices.hasNext());
	vertices.next();
	assertTrue(vertices.hasNext());
	vertices.next();
	assertFalse(vertices.hasNext());

	List<Vertex> vertices2 = graph.traversal().V().toList();
	assertEquals(2, vertices2.size());
    }

}
