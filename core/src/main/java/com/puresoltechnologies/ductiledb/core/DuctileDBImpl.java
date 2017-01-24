package com.puresoltechnologies.ductiledb.core;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.bigtable.BigTableConfiguration;
import com.puresoltechnologies.ductiledb.core.blob.BlobStore;
import com.puresoltechnologies.ductiledb.core.blob.BlobStoreImpl;
import com.puresoltechnologies.ductiledb.core.graph.GraphStore;
import com.puresoltechnologies.ductiledb.core.graph.GraphStoreImpl;
import com.puresoltechnologies.ductiledb.engine.DatabaseEngine;
import com.puresoltechnologies.ductiledb.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.api.StorageFactory;
import com.puresoltechnologies.ductiledb.storage.api.StorageFactoryServiceException;

public class DuctileDBImpl implements DuctileDB {

    private final static Logger logger = LoggerFactory.getLogger(DuctileDBImpl.class);

    private final static String DIRECTORY_NAME = "/DuctileDB";

    private final DuctileDBConfiguration configuration;
    private final BlobStoreImpl blobStore;
    private final GraphStoreImpl graph;
    private final DatabaseEngineImpl tableStore;

    private boolean closed = false;

    public DuctileDBImpl(DuctileDBConfiguration configuration) {
	try {
	    this.configuration = configuration;
	    this.blobStore = new BlobStoreImpl(configuration, DIRECTORY_NAME);
	    this.tableStore = createDatabaseEngine(configuration.getBigTableEngine(), DIRECTORY_NAME);
	    this.graph = new GraphStoreImpl(configuration.getGraph(), blobStore, tableStore, true);
	} catch (StorageFactoryServiceException | IOException e) {
	    throw new StorageException("Could not start DuctileDB.", e);
	}
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

    /**
     * Creates a new new {@link DatabaseEngine} out of the provided
     * {@link DatabaseEngineConfiguration).
     * 
     * @param configuration
     * @return
     * @throws IOException
     * @throws StorageFactoryServiceException
     */
    private static DatabaseEngineImpl createDatabaseEngine(BigTableConfiguration configuration, String directory)
	    throws StorageFactoryServiceException, IOException {
	logger.info("Creating connection to DuctileDB with configuration '" + configuration + "'...");
	DatabaseEngineImpl storageEngine = new DatabaseEngineImpl(
		StorageFactory.getStorageInstance(configuration.getStorage()), new File(directory), "ductiledb",
		configuration);
	logger.info("Connection '" + storageEngine + "' to DuctileDB created.");
	return storageEngine;
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
    public DatabaseEngine getBigTableStore() {
	return tableStore;
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
	try {
	    tableStore.close();
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
