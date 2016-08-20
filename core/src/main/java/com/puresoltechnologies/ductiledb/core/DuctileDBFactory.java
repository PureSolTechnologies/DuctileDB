package com.puresoltechnologies.ductiledb.core;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.puresoltechnologies.ductiledb.api.DuctileDB;
import com.puresoltechnologies.ductiledb.core.blob.BlobStoreImpl;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBGraphImpl;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.api.StorageFactory;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngine;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineConfiguration;
import com.puresoltechnologies.ductiledb.storage.engine.DatabaseEngineImpl;
import com.puresoltechnologies.ductiledb.storage.engine.schema.SchemaException;

/**
 * This is the central factory to connect to DuctileDB. The primary goal is to
 * have a simple point of access, because DuctileDB needs some information to
 * connect to Hadoop and HBase.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBFactory {

    private static final Logger logger = LoggerFactory.getLogger(DuctileDBFactory.class);

    /**
     * Reads the Yaml configuration out of the provided input stream.
     * 
     * @param inputStream
     * @return
     */
    public static DuctileDBConfiguration readConfiguration(InputStream inputStream) {
	Yaml yaml = new Yaml();
	return yaml.loadAs(inputStream, DuctileDBConfiguration.class);
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

    public static DuctileDB connect(URL configurationFile) throws IOException, StorageException, SchemaException {
	try (InputStream fileInputStream = configurationFile.openStream()) {
	    return connect(fileInputStream);
	}
    }

    public static DuctileDB connect(InputStream configurationFileStream)
	    throws StorageException, SchemaException, FileNotFoundException, IOException {
	DuctileDBConfiguration configuration = readConfiguration(configurationFileStream);
	return connect(configuration);
    }

    public static DuctileDB connect(DuctileDBConfiguration configuration) throws StorageException, SchemaException {
	BlobStoreImpl blobStore = new BlobStoreImpl(configuration);
	DuctileDBGraphImpl graph = createGraph(blobStore, configuration);
	return new DuctileDBImpl(blobStore, graph);
    }

    private static DuctileDBGraphImpl createGraph(BlobStoreImpl blobStore, DuctileDBConfiguration configuration)
	    throws StorageException, SchemaException {
	DatabaseEngineImpl storageEngine = createDatabaseEngine(configuration.getDatabaseEngine());
	return new DuctileDBGraphImpl(configuration.getGraph(), blobStore, storageEngine, true);
    }

}
