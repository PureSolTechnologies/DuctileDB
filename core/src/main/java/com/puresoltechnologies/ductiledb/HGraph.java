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
public interface HGraph extends TransactionalGraph, Closeable {

    public Connection getConnection();

    @Override
    public HGraphVertex addVertex(Object id);

    public HGraphVertex addVertex(Object id, Set<String> labels, Map<String, Object> properties);

    @Override
    public HGraphVertex getVertex(Object id);

    @Override
    public HGraphEdge addEdge(Object edgeId, Vertex startVertex, Vertex targetVertex, String edgeType);

    public HGraphEdge addEdge(Object edgeId, Vertex startVertex, Vertex targetVertex, String edgeType,
	    Map<String, Object> properties);

    @Override
    public void commit();

    @Override
    public void rollback();
}
