package com.puresoltechnologies.ductiledb.api.graph;

import java.io.Closeable;

import com.puresoltechnologies.ductiledb.api.graph.manager.DuctileDBGraphManager;
import com.puresoltechnologies.ductiledb.api.graph.schema.DuctileDBSchemaManager;
import com.puresoltechnologies.ductiledb.api.graph.tx.DuctileDBTransaction;

/**
 * This interface is used to define the public interface for Ductile DB graph.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface GraphStore extends DuctileDBTransaction, GraphOperations, Closeable {

    /**
     * This is the type of BLOB vertices.
     */
    public static final String BLOB_TYPE_NAME = "BLOB";
    public static final String BLOB_HASHID_PROPERTY_NAME = "blob.hashid";
    public static final String BLOB_SIZE_PROPERTY_NAME = "blob.size";

    /**
     * This method creates a {@link DuctileDBTransaction} to be used
     * independently of the thread.
     * 
     * @return A new {@link DuctileDBTransaction} is returned.
     */
    public DuctileDBTransaction createTransaction();

    /**
     * This method returns the {@link DuctileDBGraphManager} for this graph.
     * 
     * @return A {@link DuctileDBGraphManager} object is returned.
     */
    public DuctileDBGraphManager createGraphManager();

    /**
     * This method returns the {@link DuctileDBSchemaManager} which is
     * responsible to configure the graph schema.
     * 
     * @return A {@link DuctileDBSchemaManager} object is returned.
     */
    public DuctileDBSchemaManager createSchemaManager();
}
