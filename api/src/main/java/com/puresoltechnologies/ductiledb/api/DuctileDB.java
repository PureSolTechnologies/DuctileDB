package com.puresoltechnologies.ductiledb.api;

import java.io.Closeable;

import com.puresoltechnologies.ductiledb.api.blob.BlobStore;
import com.puresoltechnologies.ductiledb.api.graph.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.rdbms.RelationalDuctileDB;

/**
 * This is the central interface for DuctileDB.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface DuctileDB extends Closeable {

    public boolean isClosed();

    /**
     * This method returns the {@link DuctileDBGraph} of DuctileDB.
     * 
     * @return A {@link DuctileDBGraph} object is returned to use the graph
     *         engine.
     */
    public DuctileDBGraph getGraph();

    /**
     * This method returns the BLOB store reference to DuctileDB's BLOB store.
     * 
     * @return A {@link BlobStore} object is returned.
     */
    public BlobStore getBlobStore();

    /**
     * This method returns the RDBMS part of DuctileDB.
     * 
     * @return A {@link RelationalDuctileDB} object is returned.
     */
    public RelationalDuctileDB getRDBMS();

}
