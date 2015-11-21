package com.puresoltechnologies.ductiledb.xo.impl;

import java.lang.annotation.Annotation;

import com.buschmais.xo.spi.datastore.DatastoreEntityManager;
import com.buschmais.xo.spi.datastore.DatastoreQuery;
import com.buschmais.xo.spi.datastore.DatastoreRelationManager;
import com.buschmais.xo.spi.datastore.DatastoreSession;
import com.buschmais.xo.spi.datastore.DatastoreTransaction;
import com.buschmais.xo.spi.session.XOSession;
import com.puresoltechnologies.ductiledb.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.xo.api.annotation.Gauging;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileDBEdgeMetadata;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileDBPropertyMetadata;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileDBVertexMetadata;

/**
 * This class implements a XO DatastoreSession for Titan database.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBStoreSession implements
	DatastoreSession<Long, DuctileDBVertex, DuctileDBVertexMetadata, String, Long, DuctileDBEdge, DuctileDBEdgeMetadata, String, DuctileDBPropertyMetadata> {

    /**
     * This field contains the graph as {@link DuctileDBGraph} object.
     */
    private final DuctileDBGraph graph;
    private final DuctileDBStoreTransaction transaction;

    private final DucileDBStoreVertexManager vertexManager;
    private final DucileDBStoreEdgeManager edgeManager;

    /**
     * This is the initial value constructor.
     * 
     * @param graph
     *            is the Titan graph as TitanGraph object on which this session
     *            shall work on.
     */
    public DuctileDBStoreSession(DuctileDBGraph graph) {
	this.graph = graph;
	this.transaction = new DuctileDBStoreTransaction(graph);
	this.vertexManager = new DucileDBStoreVertexManager(graph);
	this.edgeManager = new DucileDBStoreEdgeManager(graph);
    }

    /**
     * Returns the Titan graph which is currently opened.
     * 
     * @return A TitanGraph object is returned.
     */
    public final DuctileDBGraph getGraph() {
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
    public DatastoreEntityManager<Long, DuctileDBVertex, DuctileDBVertexMetadata, String, DuctileDBPropertyMetadata> getDatastoreEntityManager() {
	return vertexManager;
    }

    @Override
    public DatastoreRelationManager<DuctileDBVertex, Long, DuctileDBEdge, DuctileDBEdgeMetadata, String, DuctileDBPropertyMetadata> getDatastoreRelationManager() {
	return edgeManager;
    }

    @Override
    public Class<? extends Annotation> getDefaultQueryLanguage() {
	return Gauging.class;
    }

    @Override
    public <QL extends Annotation> DatastoreQuery<QL> createQuery(Class<QL> queryLanguage) {
	if (!queryLanguage.equals(Gauging.class)) {
	    throw new IllegalArgumentException("Query language " + queryLanguage.getName() + " is not supported.");
	}
	@SuppressWarnings("unchecked")
	DatastoreQuery<QL> query = (DatastoreQuery<QL>) new DuctileQuery(graph);
	return query;
    }

    @Override
    public <R> R createRepository(XOSession xoSession, Class<R> type) {
	// TODO Auto-generated method stub
	return null;
    }

}
