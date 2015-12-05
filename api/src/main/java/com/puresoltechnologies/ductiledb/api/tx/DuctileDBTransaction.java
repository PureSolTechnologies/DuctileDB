package com.puresoltechnologies.ductiledb.api.tx;

import java.io.Closeable;

import com.puresoltechnologies.ductiledb.api.GraphOperations;

/**
 * This interface defines the interace for Ductile DB transactions.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface DuctileDBTransaction extends GraphOperations, Closeable {

    public void commit();

    public void rollback();

}
