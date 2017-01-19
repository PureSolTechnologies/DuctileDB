package com.puresoltechnologies.ductiledb.engine;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.puresoltechnologies.commons.misc.StopWatch;
import com.puresoltechnologies.ductiledb.bigtable.BigTable;
import com.puresoltechnologies.ductiledb.bigtable.BigTableConfiguration;
import com.puresoltechnologies.ductiledb.bigtable.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.logstore.utils.DefaultObjectMapper;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

/**
 * This class is the database engine class. It supports a schema, and multiple
 * big table storages organized in column families. It is using the
 * {@link BigTable} to store the separate tables.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DatabaseEngineImpl implements DatabaseEngine {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseEngineImpl.class);

    private boolean closed = false;
    private final Storage storage;
    private final String storageName;
    private final BigTableConfiguration configuration;
    private final File storageDirectory;
    private final Map<String, NamespaceEngineImpl> namespaceEngines = new HashMap<>();

    /**
     * Creates a new database.
     * 
     * @param storage
     * @param storageName
     * @param configuration
     * @throws IOException
     */
    public DatabaseEngineImpl(Storage storage, File storageDirectory, String storageName,
	    BigTableConfiguration configuration) throws IOException {
	this.storage = storage;
	this.storageDirectory = storageDirectory;
	if (storage.exists(storageDirectory) && storage.isDirectory(storageDirectory)) {
	    ObjectMapper objectMapper = DefaultObjectMapper.getInstance();
	    try (BufferedInputStream parameterFile = storage.open(new File(storageDirectory, "descriptor.json"))) {
		this.storageName = objectMapper.readValue(parameterFile, String.class);
	    }
	    logger.info("Starting database engine '" + storageName + "'...");
	    StopWatch stopWatch = new StopWatch();
	    stopWatch.start();
	    try (BufferedInputStream parameterFile = storage.open(new File(storageDirectory, "configuration.json"))) {
		this.configuration = objectMapper.readValue(parameterFile, BigTableConfiguration.class);
	    }
	    openNamespaces();
	    stopWatch.stop();
	    logger.info("Database engine '" + storageName + "' started in " + stopWatch.getMillis() + "ms.");
	} else {
	    logger.info("Creating database engine '" + storageName + "'...");
	    StopWatch stopWatch = new StopWatch();
	    stopWatch.start();
	    this.storageName = storageName;
	    this.configuration = configuration;
	    storage.createDirectory(storageDirectory);
	    stopWatch.stop();
	    logger.info("Database engine '" + storageName + "' created in " + stopWatch.getMillis() + "ms.");
	}
    }

    private void openNamespaces() throws IOException {
	Iterable<File> directories = storage.list(storageDirectory);
	for (File directory : directories) {
	    if (storage.isDirectory(directory)) {
		NamespaceEngineImpl engine = (NamespaceEngineImpl) NamespaceEngine.reopen(storage, directory);
		namespaceEngines.put(engine.getName(), engine);
	    }
	}
    }

    @Override
    public NamespaceEngine addNamespace(String namespace) throws IOException {
	NamespaceDescriptor descriptor = new NamespaceDescriptor(storage, new File(storageDirectory, namespace));
	NamespaceEngineImpl namespaceEngine = (NamespaceEngineImpl) NamespaceEngine.create(storage, descriptor,
		configuration);
	namespaceEngines.put(namespace, namespaceEngine);
	return namespaceEngine;
    }

    @Override
    public NamespaceEngine getNamespace(String namespaceName) {
	return namespaceEngines.get(namespaceName);
    }

    @Override
    public boolean hasNamespace(String namespaceName) {
	return namespaceEngines.containsKey(namespaceName);
    }

    public void setRunCompactions(boolean runCompaction) {
	namespaceEngines.values().forEach(engine -> engine.setRunCompactions(runCompaction));
    }

    public void runCompaction() {
	namespaceEngines.values().forEach(engine -> engine.runCompaction());
    }

    @Override
    public void close() throws IOException {
	logger.info("Closing database engine '" + storageName + "'...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	for (NamespaceEngine namespaceEngine : namespaceEngines.values()) {
	    namespaceEngine.close();
	}
	closed = true;
	storage.close();
	stopWatch.stop();
	logger.info("Database engine '" + storageName + "' closed in " + stopWatch.getMillis() + "ms.");
    }

    @Override
    public final boolean isClosed() {
	return closed;
    }

    public final Storage getStorage() {
	return storage;
    }

    @Override
    public final String getStoreName() {
	return storageName;
    }

    @Override
    public String toString() {
	return "DatabaseEngine: " + storageName;
    }
}
