package com.puresoltechnologies.ductiledb.xo.impl;

import java.lang.annotation.Annotation;

import com.buschmais.xo.spi.datastore.DatastoreEntityManager;
import com.buschmais.xo.spi.datastore.DatastoreQuery;
import com.buschmais.xo.spi.datastore.DatastoreRelationManager;
import com.buschmais.xo.spi.datastore.DatastoreSession;
import com.buschmais.xo.spi.datastore.DatastoreTransaction;
import com.buschmais.xo.spi.session.XOSession;
import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.exceptions.DuctileDBException;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileEdge;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileGraph;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileVertex;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileEdgeMetadata;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctilePropertyMetadata;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileVertexMetadata;

/**
 * This class implements a XO DatastoreSession for Titan database.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileStoreSession implements
	DatastoreSession<Long, DuctileVertex, DuctileVertexMetadata, String, Long, DuctileEdge, DuctileEdgeMetadata, String, DuctilePropertyMetadata> {

    /**
     * This field contains the graph as {@link DuctileDBGraph} object.
     */
    private final DuctileGraph graph;
    private final DuctileStoreTransaction transaction;

    private final DucileStoreVertexManager vertexManager;
    private final DucileStoreEdgeManager edgeManager;

    /**
     * This is the initial value constructor.
     * 
     * @param graph
     *            is the Titan graph as TitanGraph object on which this session
     *            shall work on.
     */
    public DuctileStoreSession(DuctileGraph graph) {
	this.graph = graph;
	this.transaction = new DuctileStoreTransaction(graph);
	this.vertexManager = new DucileStoreVertexManager(graph);
	this.edgeManager = new DucileStoreEdgeManager(graph);
    }

    /**
     * Returns the Titan graph which is currently opened.
     * 
     * @return A TitanGraph object is returned.
     */
    public final DuctileGraph getGraph() {
	return graph;
    }

    @Override
    public DatastoreTransaction getDatastoreTransaction() {
	return transaction;
    }

    @Override
    public void close() {
	// Nothing to do here...
    }

    @Override
    public DatastoreEntityManager<Long, DuctileVertex, DuctileVertexMetadata, String, DuctilePropertyMetadata> getDatastoreEntityManager() {
	return vertexManager;
    }

    @Override
    public DatastoreRelationManager<DuctileVertex, Long, DuctileEdge, DuctileEdgeMetadata, String, DuctilePropertyMetadata> getDatastoreRelationManager() {
	return edgeManager;
    }

    @Override
    public Class<? extends Annotation> getDefaultQueryLanguage() {
	throw new DuctileDBException("Queries are not supported, yet.");
    }

    @Override
    public <QL extends Annotation> DatastoreQuery<QL> createQuery(Class<QL> queryLanguage) {
	throw new DuctileDBException("Queries are not supported, yet.");
    }

    @Override
    public <R> R createRepository(XOSession xoSession, Class<R> type) {
	// TODO Auto-generated method stub
	return null;
    }

}
