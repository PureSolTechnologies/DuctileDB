package com.puresoltechnologies.ductiledb;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Connection;

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

    @Override
    public DuctileDBVertex addVertex(Object id);

    public DuctileDBVertex addVertex(Object id, Set<String> labels, Map<String, Object> properties);

    @Override
    public DuctileDBVertex getVertex(Object id);

    public Iterable<DuctileDBVertex> getVertices(String label);

    @Override
    public DuctileDBEdge addEdge(Object edgeId, Vertex startVertex, Vertex targetVertex, String edgeType);

    public DuctileDBEdge addEdge(Object edgeId, Vertex startVertex, Vertex targetVertex, String edgeType,
	    Map<String, Object> properties);

    @Override
    public DuctileDBEdge getEdge(Object edgeId);

    @Override
    public void commit();

    @Override
    public void rollback();
}
