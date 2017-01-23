package com.puresoltechnologies.ductiledb.core;

import com.puresoltechnologies.ductiledb.bigtable.BigTableConfiguration;
import com.puresoltechnologies.ductiledb.core.blob.BlobStoreConfiguration;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBGraphConfiguration;

/**
 * This object keeps all settings of DuctileDB and its subcomponents.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBConfiguration {

    private BigTableConfiguration bigTableEngine;
    private BlobStoreConfiguration blobStore;
    private DuctileDBGraphConfiguration graph;

    public BigTableConfiguration getBigTableEngine() {
	return bigTableEngine;
    }

    public void setBigTableEngine(BigTableConfiguration bigTableEngine) {
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

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((bigTableEngine == null) ? 0 : bigTableEngine.hashCode());
	result = prime * result + ((blobStore == null) ? 0 : blobStore.hashCode());
	result = prime * result + ((graph == null) ? 0 : graph.hashCode());
	return result;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (getClass() != obj.getClass())
	    return false;
	DuctileDBConfiguration other = (DuctileDBConfiguration) obj;
	if (bigTableEngine == null) {
	    if (other.bigTableEngine != null)
		return false;
	} else if (!bigTableEngine.equals(other.bigTableEngine))
	    return false;
	if (blobStore == null) {
	    if (other.blobStore != null)
		return false;
	} else if (!blobStore.equals(other.blobStore))
	    return false;
	if (graph == null) {
	    if (other.graph != null)
		return false;
	} else if (!graph.equals(other.graph))
	    return false;
	return true;
    }

}
