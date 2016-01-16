package com.puresoltechnologies.ductiledb.xo.impl;

import java.io.IOException;
import java.lang.annotation.Annotation;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;

import com.buschmais.xo.spi.datastore.DatastoreEntityManager;
import com.buschmais.xo.spi.datastore.DatastoreQuery;
import com.buschmais.xo.spi.datastore.DatastoreRelationManager;
import com.buschmais.xo.spi.datastore.DatastoreSession;
import com.buschmais.xo.spi.datastore.DatastoreTransaction;
import com.buschmais.xo.spi.session.XOSession;
import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileEdge;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileGraph;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileGraphFactory;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileVertex;
import com.puresoltechnologies.ductiledb.xo.api.annotation.Query;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileEdgeMetadata;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctilePropertyMetadata;
import com.puresoltechnologies.ductiledb.xo.impl.metadata.DuctileVertexMetadata;

/**
 * This class implements a XO DatastoreSession for DuctileDB database.
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
     *            is the DuctileDB graph as {@link DuctileGraph} object on which
     *            this session shall work on.
     * @throws IOException
     */
    public DuctileStoreSession(Connection connection, BaseConfiguration configuration) throws IOException {
	this.graph = DuctileGraphFactory.createGraph(connection, configuration);
	this.transaction = new DuctileStoreTransaction(graph);
	this.vertexManager = new DucileStoreVertexManager(graph);
	this.edgeManager = new DucileStoreEdgeManager(graph);
    }

    /**
     * Returns the DuctileDB graph which is currently opened.
     * 
     * @return A {@link DuctileGraph} object is returned.
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
	return Query.class;

    }

    @Override
    public <QL extends Annotation> DatastoreQuery<QL> createQuery(Class<QL> queryLanguage) {
	if (!queryLanguage.equals(Query.class)) {
	    throw new IllegalArgumentException("Query language " + queryLanguage.getName() + " is not supported.");
	}
	@SuppressWarnings("unchecked")
	DatastoreQuery<QL> query = (DatastoreQuery<QL>) new DuctileDatastoreQuery(graph);
	return query;
    }

    @Override
    public <R> R createRepository(XOSession xoSession, Class<R> type) {
	// TODO Auto-generated method stub
	return null;
    }

}
