package com.puresoltechnologies.ductiledb.engine;

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
import com.puresoltechnologies.ductiledb.storage.spi.StorageInputStream;
import com.puresoltechnologies.ductiledb.storage.spi.StorageOutputStream;

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
	logger.info("Starting database engine '" + storageName + "'...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();

	ObjectMapper objectMapper = DefaultObjectMapper.getInstance();
	File descriptorFile = new File(storageDirectory, "descriptor.json");
	File configurationFile = new File(storageDirectory, "configuration.json");
	if (!storage.exists(storageDirectory)) {
	    storage.createDirectory(storageDirectory);
	}
	if (storage.exists(descriptorFile)) {
	    try (StorageInputStream parameterFile = storage.open(descriptorFile)) {
		this.storageName = objectMapper.readValue(parameterFile, String.class);
	    }
	} else {
	    this.storageName = storageName;
	    try (StorageOutputStream parameterFile = storage.create(descriptorFile)) {
		objectMapper.writeValue(parameterFile, storageName);
	    }
	}
	if (storage.exists(configurationFile)) {
	    try (StorageInputStream parameterFile = storage.open(configurationFile)) {
		this.configuration = objectMapper.readValue(parameterFile, BigTableConfiguration.class);
	    }
	} else {
	    this.configuration = configuration;
	    try (StorageOutputStream parameterFile = storage.create(configurationFile)) {
		objectMapper.writeValue(parameterFile, configuration);
	    }
	}
	openNamespaces();
	stopWatch.stop();
	logger.info("Database engine '" + storageName + "' started in " + stopWatch.getMillis() + "ms.");
    }

    private void openNamespaces() throws IOException {
	Iterable<File> directories = storage.list(storageDirectory);
	for (File directory : directories) {
	    if (storage.isDirectory(directory)) {
		if (storage.exists(new File(directory, "descriptor.json"))) {
		    NamespaceImpl engine = (NamespaceImpl) Namespace.reopen(storage, directory);
		    namespaceEngines.put(engine.getName(), engine);
		}
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
