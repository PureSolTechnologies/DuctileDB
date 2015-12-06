package com.puresoltechnologies.ductiledb.api;

import java.io.Closeable;
import java.io.IOException;

import com.puresoltechnologies.ductiledb.api.tx.DuctileDBTransaction;

/**
 * This interface is used to define the public interface for Ductile DB graph.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface DuctileDBGraph extends GraphOperations, Closeable {

    /**
     * This method creates a {@link DuctileDBTransaction} to be used
     * independently of the thread.
     * 
     * @return A new {@link DuctileDBTransaction} is returned.
     */
    public DuctileDBTransaction createTransaction();

    /**
     * This method is used to trigger the persistence of the current changes in
     * the thread local transaction.
     * 
     * @throws IOException
     *             is thrown in case of an database issue.
     */
    public void commit() throws IOException;

    /**
     * This method is used to drop the current changes in the thread local
     * transaction. No graph changes take place.
     * 
     * @throws IOException
     *             is thrown in case of an database issue.
     */
    public void rollback() throws IOException;

}
