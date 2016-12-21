package com.puresoltechnologies.ductiledb.core;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.blobstore.BlobStore;
import com.puresoltechnologies.ductiledb.blobstore.BlobStoreImpl;
import com.puresoltechnologies.ductiledb.core.graph.GraphStore;
import com.puresoltechnologies.ductiledb.core.graph.GraphStoreImpl;

public class DuctileDBImpl implements DuctileDB {

    private final static Logger logger = LoggerFactory.getLogger(DuctileDBImpl.class);

    private final Connection connection;
    private final DuctileDBConfiguration configuration;
    private final BlobStoreImpl blobStore;
    private final GraphStoreImpl graph;

    private boolean closed = false;

    public DuctileDBImpl(DuctileDBConfiguration configuration, Connection connection) throws SQLException {
	this.configuration = configuration;
	this.connection = connection;
	this.blobStore = new BlobStoreImpl(configuration, connection);
	this.graph = new GraphStoreImpl(configuration.getGraph(), blobStore, connection, true);
    }

    @Override
    public boolean isStopped() {
	return closed;
    }

    /**
     * Returns the database configuration.
     * 
     * @return A {@link DuctileDBConfiguration} object is returned.
     */
    public DuctileDBConfiguration getConfiguration() {
	return configuration;
    }

    @Override
    public GraphStore getGraph() {
	return graph;
    }

    @Override
    public BlobStore getBlobStore() {
	return blobStore;
    }

    @Override
    public Connection getConnection() {
	return connection;
    }

    @Override
    public void close() {
	try {
	    graph.close();
	} catch (Exception e) {
	    logger.warn("Could not close graph.", e);
	}
	try {
	    connection.close();
	} catch (Exception e) {
	    logger.warn("Could not close graph.", e);
	}
	closed = true;
	logger.info("DuctileDB closed.");
    }
}
