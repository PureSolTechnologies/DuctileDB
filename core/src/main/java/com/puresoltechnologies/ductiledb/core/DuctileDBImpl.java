package com.puresoltechnologies.ductiledb.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.api.DuctileDB;
import com.puresoltechnologies.ductiledb.api.blob.BlobStore;
import com.puresoltechnologies.ductiledb.api.graph.DuctileDBGraph;

public class DuctileDBImpl implements DuctileDB {

    private final Logger logger = LoggerFactory.getLogger(DuctileDBImpl.class);

    private final BlobStore blobStore;
    private final DuctileDBGraph graph;

    public DuctileDBImpl(BlobStore blobStore, DuctileDBGraph graph) {
	this.blobStore = blobStore;
	this.graph = graph;
	logger.info("DuctileDB initialized.");
    }

    @Override
    public DuctileDBGraph getGraph() {
	return graph;
    }

    @Override
    public BlobStore getBlobStore() {
	return blobStore;
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
	logger.info("DuctileDB closed.");
    }
}