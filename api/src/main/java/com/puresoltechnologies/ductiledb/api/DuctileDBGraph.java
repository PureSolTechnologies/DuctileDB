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

    public DuctileDBTransaction getCurrentTransaction();

    public void commit() throws IOException;

    public void rollback() throws IOException;

}
