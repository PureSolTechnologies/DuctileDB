package com.puresoltechnologies.ductiledb.core;

import java.io.Closeable;

import com.puresoltechnologies.ductiledb.core.blob.BlobStore;
import com.puresoltechnologies.ductiledb.core.graph.GraphStore;
import com.puresoltechnologies.ductiledb.core.tables.TableStore;

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
     * This method returns the {@link GraphStore} of DuctileDB.
     * 
     * @return A {@link GraphStore} object is returned to use the graph
     *         engine.
     */
    public GraphStore getGraph();

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
