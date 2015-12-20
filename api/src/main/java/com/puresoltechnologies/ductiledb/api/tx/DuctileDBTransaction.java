package com.puresoltechnologies.ductiledb.api.tx;

import java.io.Closeable;
import java.util.function.Consumer;

import com.puresoltechnologies.ductiledb.api.GraphOperations;

/**
 * This interface is used for Ductile DB transactions.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface DuctileDBTransaction extends GraphOperations, Closeable {

    /**
     * This enum defines the status of the transaction.
     * 
     * @author Rick-Rainer Ludwig
     */
    public static enum Status {
	COMMIT, ROLLBACK;
    };

    public void commit();

    public void rollback();

    public boolean isOpen();

    public void addTransactionListener(final Consumer<Status> listener);

    public void removeTransactionListener(final Consumer<Status> listener);

    public void clearTransactionListeners();
}
