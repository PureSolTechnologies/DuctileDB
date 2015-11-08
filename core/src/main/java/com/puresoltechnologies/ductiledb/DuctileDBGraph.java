package com.puresoltechnologies.ductiledb;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Connection;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;

/**
 * This is the central interface for HGraph graphs. It is an extension for
 * TinkerPop's Graph interface to enhance HGraph with functionality not present
 * in the generic graph model.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface DuctileDBGraph extends TransactionalGraph, Closeable {

    public Connection getConnection();

    public DuctileDBVertex addVertex();

    @Override
    public DuctileDBVertex addVertex(Object id);

    public DuctileDBVertex addVertex(Set<String> labels, Map<String, Object> properties);

    public DuctileDBVertex getVertex(long vertexId);

    @Override
    public DuctileDBVertex getVertex(Object id);

    public Iterable<DuctileDBVertex> getVertices(String label);

    public DuctileDBEdge addEdge(Vertex startVertex, Vertex targetVertex, String edgeType);

    @Override
    public DuctileDBEdge addEdge(Object edgeId, Vertex startVertex, Vertex targetVertex, String edgeType);

    public DuctileDBEdge addEdge(Vertex startVertex, Vertex targetVertex, String edgeType,
	    Map<String, Object> properties);

    public DuctileDBEdge getEdge(long edgeId);

    @Override
    public DuctileDBEdge getEdge(Object edgeId);

    public Iterable<Edge> getEdges(String edgeType);

    @Override
    public void commit();

    @Override
    public void rollback();
}
