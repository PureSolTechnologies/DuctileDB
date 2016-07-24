package com.puresoltechnologies.ductiledb.storage.engine;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.commons.misc.StopWatch;
import com.puresoltechnologies.ductiledb.storage.api.StorageException;
import com.puresoltechnologies.ductiledb.storage.engine.schema.NamespaceDescriptor;
import com.puresoltechnologies.ductiledb.storage.engine.schema.TableDescriptor;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class NamespaceEngine implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(NamespaceEngine.class);

    private final Storage storage;
    private final NamespaceDescriptor namespaceDescriptor;
    private final DatabaseEngineConfiguration configuration;
    private final Map<String, TableEngine> tableEngines = new HashMap<>();

    public NamespaceEngine(Storage storage, NamespaceDescriptor namespaceDescriptor,
	    DatabaseEngineConfiguration configuration) throws StorageException {
	this.storage = storage;
	this.namespaceDescriptor = namespaceDescriptor;
	this.configuration = configuration;
	logger.info("Starting namespace engine '" + namespaceDescriptor.getName() + "'...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	initializeTableEngines();
	stopWatch.stop();
	logger.info(
		"Namespace engine '" + namespaceDescriptor.getName() + "' started in " + stopWatch.getMillis() + "ms.");
    }

    private void initializeTableEngines() throws StorageException {
	for (TableDescriptor tableDescriptor : namespaceDescriptor.getTables()) {
	    tableEngines.put(tableDescriptor.getName(), new TableEngine(storage, tableDescriptor, configuration));
	}
    }

    @Override
    public void close() throws IOException {
	logger.info("Closing namespace engine '" + namespaceDescriptor.getName() + "'...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	for (TableEngine tableEngine : tableEngines.values()) {
	    tableEngine.close();
	}
	storage.close();
	stopWatch.stop();
	logger.info(
		"Namespace engine '" + namespaceDescriptor.getName() + "' closed in " + stopWatch.getMillis() + "ms.");
    }
}
