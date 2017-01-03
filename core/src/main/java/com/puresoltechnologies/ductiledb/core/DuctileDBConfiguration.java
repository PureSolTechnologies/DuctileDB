package com.puresoltechnologies.ductiledb.core;

import com.puresoltechnologies.ductiledb.bigtable.BigTableEngineConfiguration;
import com.puresoltechnologies.ductiledb.core.blob.BlobStoreConfiguration;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBGraphConfiguration;
import com.puresoltechnologies.ductiledb.core.tables.TableStoreConfiguration;

/**
 * This object keeps all settings of DuctileDB and its subcomponents.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBConfiguration {

    private BigTableEngineConfiguration bigTableEngine;
    private BlobStoreConfiguration blobStore;
    private DuctileDBGraphConfiguration graph;
    private TableStoreConfiguration tableStore;

    public BigTableEngineConfiguration getBigTableEngine() {
	return bigTableEngine;
    }

    public void setBigTableEngine(BigTableEngineConfiguration bigTableEngine) {
	this.bigTableEngine = bigTableEngine;
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
