package com.puresoltechnologies.ductiledb.core.graph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.api.graph.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.core.DuctileDBConfiguration;
import com.puresoltechnologies.ductiledb.core.blob.BlobStoreImpl;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.api.StorageFactory;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngine;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineConfiguration;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;

public class DuctileDBGraphFactory {

    private static final Logger logger = LoggerFactory.getLogger(DuctileDBGraphFactory.class);

    public static DatabaseEngine createDatabaseEngine(DatabaseEngineConfiguration configuration)
	    throws StorageException {
	// TODO incorporate configuration...
	logger.info("Creating connection to DuctileDB with configuration '" + configuration + "'...");
	DatabaseEngine storageEngine = new DatabaseEngineImpl(
		StorageFactory.getStorageInstance(configuration.getStorage()), "graph", configuration);
	logger.info("Connection '" + storageEngine + "' to DuctileDB created.");
	return storageEngine;
    }

    public static DuctileDBGraph createGraph(BlobStoreImpl blobStore, DuctileDBConfiguration configuration)
	    throws StorageException, SchemaException {
	DatabaseEngine storageEngine = createDatabaseEngine(configuration.getDatabaseEngine());
	return new DuctileDBGraphImpl(blobStore, storageEngine, true);
    }

}
