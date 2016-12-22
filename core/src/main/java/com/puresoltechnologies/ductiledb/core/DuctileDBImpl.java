package com.puresoltechnologies.ductiledb.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.core.blob.BlobStore;
import com.puresoltechnologies.ductiledb.core.blob.BlobStoreImpl;
import com.puresoltechnologies.ductiledb.core.graph.GraphStore;
import com.puresoltechnologies.ductiledb.core.graph.GraphStoreImpl;
import com.puresoltechnologies.ductiledb.core.tables.TableStore;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreImpl;
import com.puresoltechnologies.ductiledb.engine.DatabaseEngine;
import com.puresoltechnologies.ductiledb.engine.DatabaseEngineConfiguration;
import com.puresoltechnologies.ductiledb.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.engine.schema.SchemaException;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.api.StorageFactory;

public class DuctileDBImpl implements DuctileDB {

    private final static Logger logger = LoggerFactory.getLogger(DuctileDBImpl.class);

    private final DuctileDBConfiguration configuration;
    private final BlobStoreImpl blobStore;
    private final GraphStoreImpl graph;
    private final TableStoreImpl tableStore;

    private boolean closed = false;

    public DuctileDBImpl(DuctileDBConfiguration configuration) {
	try {
	    this.configuration = configuration;
	    this.blobStore = new BlobStoreImpl(configuration);
	    DatabaseEngineImpl graphStorageEngine = createDatabaseEngine(configuration.getDatabaseEngine(),
		    GraphStoreImpl.STORAGE_DIRECTORY);
	    this.graph = new GraphStoreImpl(configuration.getGraph(), blobStore, graphStorageEngine, true);
	    DatabaseEngineImpl tablesStorageEngine = createDatabaseEngine(configuration.getDatabaseEngine(),
		    TableStoreImpl.STORAGE_DIRECTORY);
	    this.tableStore = new TableStoreImpl(configuration.getTableStore(), tablesStorageEngine, true);
	} catch (SchemaException e) {
	    throw new StorageException("Could not create graph instance.", e);
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
     */
    private static DatabaseEngineImpl createDatabaseEngine(DatabaseEngineConfiguration configuration,
	    String directory) {
	logger.info("Creating connection to DuctileDB with configuration '" + configuration + "'...");
	DatabaseEngineImpl storageEngine = new DatabaseEngineImpl(
		StorageFactory.getStorageInstance(configuration.getStorage()), directory, configuration);
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
    public TableStore getTableStore() {
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
