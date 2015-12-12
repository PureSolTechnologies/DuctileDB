package com.puresoltechnologies.ductiledb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.apache.commons.configuration.BaseConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.utils.ElementUtils;

public class AbstractDuctileDBGraphTest {

    protected static DuctileDBGraph graph;

    @BeforeClass
    public static void connect() throws IOException {
	graph = GraphFactory.createGraph(new BaseConfiguration());
	assertNotNull("Graph was not created.", graph);
	DuctileDBTestHelper.removeGraph(graph);
    }

    @AfterClass
    public static void disconnect() throws IOException {
	graph.close();
	graph = null;
    }

    protected void assertInTransaction(DuctileDBVertex vertex) {
	DuctileDBVertex readVertex = graph.getVertex(vertex.getId());
	assertEquals(vertex, readVertex);
    }

    protected void assertUnequalInTransaction(DuctileDBVertex vertex) {
	assertNotNull(graph.getVertex(vertex.getId()));
    }

    protected void assertNotInTransaction(DuctileDBVertex vertex) {
	assertNull(graph.getVertex(vertex.getId()));
    }

    protected void assertInGraph(DuctileDBVertex vertex) {
	DuctileDBVertex readVertex = graph.createTransaction().getVertex(vertex.getId());
	assertNotNull(readVertex);
	assertEquals(vertex, readVertex);
    }

    protected void assertInGraphWithDifferentEdges(DuctileDBVertex vertex) {
	DuctileDBVertex readVertex = graph.createTransaction().getVertex(vertex.getId());
	assertNotNull(readVertex);
	assertEquals(vertex.getId(), readVertex.getId());
	assertEquals(ElementUtils.getLabels(vertex), ElementUtils.getLabels(readVertex));
	assertEquals(ElementUtils.getProperties(vertex), ElementUtils.getProperties(readVertex));
    }

    protected void assertUnequalInGraph(DuctileDBVertex vertex) {
	DuctileDBVertex readVertex = graph.createTransaction().getVertex(vertex.getId());
	assertNotNull(readVertex);
	assertFalse(vertex.equals(readVertex));
    }

    protected void assertNotInGraph(DuctileDBVertex vertex) {
	assertNull(graph.createTransaction().getVertex(vertex.getId()));
    }

    protected void assertInTransaction(DuctileDBEdge edge) {
	DuctileDBEdge readEdge = graph.getEdge(edge.getId());
	assertEquals(edge, readEdge);
    }

    protected void assertUnequalInTransaction(DuctileDBEdge edge) {
	assertNotNull(graph.getEdge(edge.getId()));
    }

    protected void assertNotInTransaction(DuctileDBEdge edge) {
	assertNull(graph.getEdge(edge.getId()));
    }

    protected void assertInGraph(DuctileDBEdge edge) {
	DuctileDBEdge readEdge = graph.createTransaction().getEdge(edge.getId());
	assertNotNull(readEdge);
	assertEquals(edge, readEdge);
    }

    protected void assertUnequalInGraph(DuctileDBEdge edge) {
	DuctileDBEdge readEdge = graph.createTransaction().getEdge(edge.getId());
	assertNotNull(readEdge);
	assertFalse(edge.equals(readEdge));
    }

    protected void assertNotInGraph(DuctileDBEdge edge) {
	assertNull(graph.createTransaction().getEdge(edge.getId()));
    }
}
