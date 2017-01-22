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
import com.puresoltechnologies.ductiledb.bigtable.BigTableImpl;
import com.puresoltechnologies.ductiledb.bigtable.TableDescriptor;
import com.puresoltechnologies.ductiledb.logstore.utils.DefaultObjectMapper;
import com.puresoltechnologies.ductiledb.storage.spi.Storage;

public class NamespaceImpl implements Namespace {

    private static final Logger logger = LoggerFactory.getLogger(Namespace.class);

    private final Storage storage;
    private final NamespaceDescriptor namespaceDescriptor;
    private final BigTableConfiguration configuration;
    private final Map<String, BigTableImpl> tableEngines = new HashMap<>();

    NamespaceImpl(Storage storage, NamespaceDescriptor namespaceDescriptor, BigTableConfiguration configuration)
	    throws IOException {
	this.storage = storage;
	this.namespaceDescriptor = namespaceDescriptor;
	this.configuration = configuration;
	logger.info("Starting namespace engine '" + namespaceDescriptor.getName() + "'...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	ObjectMapper objectMapper = DefaultObjectMapper.getInstance();
	storage.createDirectory(namespaceDescriptor.getDirectory());
	try (BufferedOutputStream parameterFile = storage
		.create(new File(namespaceDescriptor.getDirectory(), "configuration.json"))) {
	    objectMapper.writeValue(parameterFile, configuration);
	}
	try (BufferedOutputStream parameterFile = storage
		.create(new File(namespaceDescriptor.getDirectory(), "descriptor.json"))) {
	    objectMapper.writeValue(parameterFile, namespaceDescriptor);
	}
	stopWatch.stop();
	logger.info(
		"Namespace engine '" + namespaceDescriptor.getName() + "' started in " + stopWatch.getMillis() + "ms.");
    }

    NamespaceImpl(Storage storage, File directory) throws IOException {
	this.storage = storage;
	ObjectMapper objectMapper = DefaultObjectMapper.getInstance();
	try (BufferedInputStream parameterFile = storage.open(new File(directory, "descriptor.json"))) {
	    this.namespaceDescriptor = objectMapper.readValue(parameterFile, NamespaceDescriptor.class);
	}
	logger.info("Starting namespace engine '" + namespaceDescriptor.getName() + "'...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	try (BufferedInputStream parameterFile = storage.open(new File(directory, "configuration.json"))) {
	    this.configuration = objectMapper.readValue(parameterFile, BigTableConfiguration.class);
	}
	openTables();
	stopWatch.stop();
	logger.info(
		"Namespace engine '" + namespaceDescriptor.getName() + "' started in " + stopWatch.getMillis() + "ms.");
    }

    private void openTables() throws IOException {
	Iterable<File> directories = storage.list(namespaceDescriptor.getDirectory());
	for (File directory : directories) {
	    if (storage.isDirectory(directory)) {
		BigTableImpl engine = (BigTableImpl) BigTable.reopen(storage, directory);
		tableEngines.put(engine.getName(), engine);
	    }
	}
    }

    @Override
    public String getName() {
	return namespaceDescriptor.getName();
    }

    @Override
    public NamespaceDescriptor getDescriptor() {
	return namespaceDescriptor;
    }

    public void setRunCompactions(boolean runCompaction) {
	tableEngines.values().forEach(engine -> engine.setRunCompactions(runCompaction));
    }

    @Override
    public Set<String> getTables() {
	Set<String> tableNames = new HashSet<>();
	for (BigTable table : tableEngines.values()) {
	    tableNames.add(table.getName());
	}
	return tableNames;
    }

    @Override
    public BigTable addTable(String name, String description) throws IOException {
	TableDescriptor tableDescriptor = new TableDescriptor(name, description,
		new File(namespaceDescriptor.getDirectory(), name));
	BigTableImpl tableEngine = (BigTableImpl) BigTable.create(storage, tableDescriptor, configuration);
	tableEngines.put(tableDescriptor.getName(), tableEngine);
	return tableEngine;
    }

    @Override
    public BigTable getTable(String name) {
	return tableEngines.get(name);
    }

    @Override
    public boolean hasTable(String name) {
	return tableEngines.containsKey(name);
    }

    @Override
    public void dropTable(String name) {
	BigTableImpl tableEngine = tableEngines.get(name);
	if (tableEngine != null) {
	    tableEngines.remove(name);
	    tableEngine.close();
	}
    }

    public void runCompaction() {
	tableEngines.values().forEach(engine -> engine.runCompaction());
    }

    @Override
    public void close() {
	logger.info("Closing namespace engine '" + namespaceDescriptor.getName() + "'...");
	StopWatch stopWatch = new StopWatch();
	stopWatch.start();
	for (BigTableImpl tableEngine : tableEngines.values()) {
	    tableEngine.close();
	}
	stopWatch.stop();
	logger.info(
		"Namespace engine '" + namespaceDescriptor.getName() + "' closed in " + stopWatch.getMillis() + "ms.");
    }
}
