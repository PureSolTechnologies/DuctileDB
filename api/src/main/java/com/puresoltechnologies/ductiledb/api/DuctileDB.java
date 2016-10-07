package com.puresoltechnologies.ductiledb.api;

import java.io.Closeable;

import com.puresoltechnologies.ductiledb.api.blob.BlobStore;
import com.puresoltechnologies.ductiledb.api.graph.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.rdbms.TableStore;

/**
 * This is the central interface for DuctileDB.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface DuctileDB extends Closeable {

    /**
     * Checks whether the database was stoped already.
     * 
     * @return
     */
    public boolean isStopped();

    /**
     * This method returns the {@link DuctileDBGraph} of DuctileDB.
     * 
     * @return A {@link DuctileDBGraph} object is returned to use the graph
     *         engine.
     */
    public DuctileDBGraph getGraph();

    /**
     * This method returns the big table store reference.
     * 
     * @return A {@link BlobStore} object is returned.
     */
    public BlobStore getBlobStore();

    /**
     * This method returns the RDBMS part of DuctileDB.
     * 
     * @return A {@link TableStore} object is returned.
     */
    public TableStore getTableStore();

}
