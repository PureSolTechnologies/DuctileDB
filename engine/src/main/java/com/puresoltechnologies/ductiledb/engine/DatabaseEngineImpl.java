package com.puresoltechnologies.ductiledb.engine;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.puresoltechnologies.commons.misc.StopWatch;
import com.puresoltechnologies.ductiledb.bigtable.BigTable;
import com.puresoltechnologies.ductiledb.bigtable.BigTableConfiguration;
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
    private final Map<String, NamespaceImpl> namespaceEngines = new HashMap<>();

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
	ObjectMapper objectMapper = DefaultObjectMapper.getInstance();
	if (storage.exists(storageDirectory) && storage.isDirectory(storageDirectory)) {
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
	    try (BufferedOutputStream parameterFile = storage.create(new File(storageDirectory, "descriptor.json"))) {
		objectMapper.writeValue(parameterFile, storageName);
	    }
	    try (BufferedOutputStream parameterFile = storage
		    .create(new File(storageDirectory, "configuration.json"))) {
		objectMapper.writeValue(parameterFile, configuration);
	    }
	    stopWatch.stop();
	    logger.info("Database engine '" + storageName + "' created in " + stopWatch.getMillis() + "ms.");
	}
    }

    private void openNamespaces() throws IOException {
	Iterable<File> directories = storage.list(storageDirectory);
	for (File directory : directories) {
	    if (storage.isDirectory(directory)) {
		NamespaceImpl engine = (NamespaceImpl) Namespace.reopen(storage, directory);
		namespaceEngines.put(engine.getName(), engine);
	    }
	}
    }

    @Override
    public Namespace addNamespace(String namespace) throws IOException {
	NamespaceDescriptor descriptor = new NamespaceDescriptor(new File(storageDirectory, namespace));
	NamespaceImpl namespaceEngine = (NamespaceImpl) Namespace.create(storage, descriptor, configuration);
	namespaceEngines.put(namespace, namespaceEngine);
	return namespaceEngine;
    }

    @Override
    public Set<String> getNamespaces() {
	Set<String> namespaceNames = new HashSet<>();
	for (Namespace namespace : namespaceEngines.values()) {
	    namespaceNames.add(namespace.getName());
	}
	return namespaceNames;
    }

    @Override
    public Namespace getNamespace(String namespaceName) {
	return namespaceEngines.get(namespaceName);
    }

    @Override
    public boolean hasNamespace(String namespaceName) {
	return namespaceEngines.containsKey(namespaceName);
    }

    @Override
    public void dropNamespace(String namespaceName) throws IOException {
	Namespace namespace = namespaceEngines.get(namespaceName);
	try {
	    namespace.close();
	    storage.removeDirectory(namespace.getDescriptor().getDirectory(), true);
	} finally {
	    namespaceEngines.remove(namespaceName);
	}
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
	for (NamespaceImpl namespaceEngine : namespaceEngines.values()) {
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
