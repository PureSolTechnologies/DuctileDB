package com.puresoltechnologies.ductiledb.core.graph;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.api.graph.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.core.blob.BlobStoreImpl;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.api.StorageFactory;
import com.puresoltechnologies.ductiledb.storage.engine.StorageEngine;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;

public class DuctileDBGraphFactory {

    private static final Logger logger = LoggerFactory.getLogger(DuctileDBGraphFactory.class);

    public static StorageEngine createConnection(Map<String, String> configuration) throws StorageException {
	// TODO incorporate configuration...
	logger.info("Creating connection to HBase with configuration '" + configuration + "'...");
	StorageEngine storageEngine = new StorageEngine(StorageFactory.getStorageInstance(configuration), "graph");
	logger.info("Connection '" + storageEngine + "' to HBase created.");
	return storageEngine;
    }

    public static DuctileDBGraph createGraph(BlobStoreImpl blobStore, Map<String, String> configuration)
	    throws StorageException, SchemaException {
	StorageEngine storageEngine = createConnection(configuration);
	return new DuctileDBGraphImpl(blobStore, storageEngine, true);
    }

}
