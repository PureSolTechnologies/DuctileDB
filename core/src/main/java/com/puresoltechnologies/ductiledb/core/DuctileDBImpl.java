package com.puresoltechnologies.ductiledb.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.api.DuctileDB;
import com.puresoltechnologies.ductiledb.api.blob.BlobStore;
import com.puresoltechnologies.ductiledb.api.graph.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.core.blob.BlobStoreImpl;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.api.StorageFactory;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngine;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineConfiguration;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;

public class DuctileDBImpl implements DuctileDB {

    private final static Logger logger = LoggerFactory.getLogger(DuctileDBImpl.class);

    private final DuctileDBConfiguration configuration;
    private final BlobStoreImpl blobStore;
    private final DuctileDBGraphImpl graph;

    private boolean closed = false;;

    public DuctileDBImpl(DuctileDBConfiguration configuration) throws StorageException {
	this.configuration = configuration;
	this.blobStore = new BlobStoreImpl(configuration);
	DatabaseEngineImpl storageEngine = createDatabaseEngine(configuration.getDatabaseEngine());
	try {
	    this.graph = new DuctileDBGraphImpl(configuration.getGraph(), blobStore, storageEngine, true);
	} catch (SchemaException e) {
	    throw new StorageException("Could not create graph instance.", e);
	}
    }

    @Override
    public boolean isClosed() {
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

    /**
     * Creates a new new {@link DatabaseEngine} out of the provided
     * {@link DatabaseEngineConfiguration).
     * 
     * @param configuration
     * @return
     * @throws StorageException
     */
    private static DatabaseEngineImpl createDatabaseEngine(DatabaseEngineConfiguration configuration)
	    throws StorageException {
	logger.info("Creating connection to DuctileDB with configuration '" + configuration + "'...");
	DatabaseEngineImpl storageEngine = new DatabaseEngineImpl(
		StorageFactory.getStorageInstance(configuration.getStorage()), "graph", configuration);
	logger.info("Connection '" + storageEngine + "' to DuctileDB created.");
	return storageEngine;
    }

    @Override
    public DuctileDBGraph getGraph() {
	return graph;
    }

    @Override
    public BlobStore getBlobStore() {
	return blobStore;
    }

    @Override
    public void close() {
	logger.info("Closing DuctileDB...");
	try {
	    blobStore.close();
	} catch (Exception e) {
	    logger.warn("Could not close BLOB store.", e);
	}
	try {
	    graph.close();
	} catch (Exception e) {
	    logger.warn("Could not close graph.", e);
	}
	closed = true;
	logger.info("DuctileDB closed.");
    }

    public void runCompaction() {
	graph.runCompaction();
    }
}
