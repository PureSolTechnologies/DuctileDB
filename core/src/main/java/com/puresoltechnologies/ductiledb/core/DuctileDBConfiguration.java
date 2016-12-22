package com.puresoltechnologies.ductiledb.core;

import com.puresoltechnologies.ductiledb.core.blob.BlobStoreConfiguration;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBGraphConfiguration;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreConfiguration;
import com.puresoltechnologies.ductiledb.engine.DatabaseEngineConfiguration;

/**
 * This object keeps all settings of DuctileDB and its subcomponents.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBConfiguration {

    private DatabaseEngineConfiguration databaseEngine;
    private BlobStoreConfiguration blobStore;
    private DuctileDBGraphConfiguration graph;
    private TableStoreConfiguration tableStore;

    public DatabaseEngineConfiguration getDatabaseEngine() {
	return databaseEngine;
    }

    public void setDatabaseEngine(DatabaseEngineConfiguration databaseEngine) {
	this.databaseEngine = databaseEngine;
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

    public TableStoreConfiguration getTableStore() {
	return tableStore;
    }

    public void setTableStore(TableStoreConfiguration rdbms) {
	this.tableStore = rdbms;
    }

}
