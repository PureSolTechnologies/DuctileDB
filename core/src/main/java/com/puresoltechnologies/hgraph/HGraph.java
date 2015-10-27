package com.puresoltechnologies.hgraph;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Connection;

import com.tinkerpop.blueprints.TransactionalGraph;

/**
 * This is the central interface for HGraph graphs. It is an extension for
 * TinkerPop's Graph interface to enhance HGraph with functionality not present
 * in the generic graph model.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface HGraph extends TransactionalGraph, Closeable {

    @Override
    public HGraphVertex addVertex(Object id);

    public HGraphVertex addVertex(Object id, Set<String> labels, Map<String, Object> properties);

    @Override
    public HGraphVertex getVertex(Object id);

    public Connection getConnection();

    @Override
    public void commit();

    @Override
    public void rollback();
}
