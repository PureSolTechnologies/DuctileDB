package com.puresoltechnologies.ductiledb.core;

import com.puresoltechnologies.ductiledb.backend.DatabaseConfiguration;
import com.puresoltechnologies.ductiledb.blobstore.BlobStoreConfiguration;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBGraphConfiguration;

/**
 * This object keeps all settings of DuctileDB and its subcomponents.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBConfiguration {

    private DatabaseConfiguration database;
    private BlobStoreConfiguration blobStore;
    private DuctileDBGraphConfiguration graph;

    public DatabaseConfiguration getDatabase() {
	return database;
    }

    public void setDatabaseEngine(DatabaseConfiguration database) {
	this.database = database;
    }

    public BlobStoreConfiguration getBlobStore() {
	return blobStore;
    }

    public void setBlobStore(BlobStoreConfiguration blobStore) {
	this.blobStore = blobStore;
    }

    public DuctileDBGraphConfiguration getGraph() {
	return graph;
    }

    public void setGraph(DuctileDBGraphConfiguration graph) {
	this.graph = graph;
    }

}
