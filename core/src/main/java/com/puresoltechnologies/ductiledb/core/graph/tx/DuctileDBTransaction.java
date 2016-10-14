package com.puresoltechnologies.ductiledb.core.graph.tx;

import java.io.Closeable;
import java.util.function.Consumer;

import com.puresoltechnologies.ductiledb.core.graph.GraphOperations;

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

    /**
     * Commits this transaction and performs all changes to graph as defined
     * before.
     */
    public void commit();

    /**
     * Rolls this transaction back by dropping all requested changes without
     * performing any change on the graph.
     */
    public void rollback();

    /**
     * This method returns whether this transaction is already or still open. A
     * closed transaction cannot be used anymore and a new transaction needs to
     * be opened.
     * 
     * @return <code>true</code> is returned in case the transaction is open and
     *         ready for changes. <code>false</code> is returned otherwise.
     */
    public boolean isOpen();

    /**
     * This method returns the current type of the transaction.
     * 
     * @return A {@link TransactionType} is returned describing the current type
     *         of transaction.
     */
    public TransactionType getTransactionType();

    /**
     * This method adds a transaction Listener for commit and rollback actions.
     * 
     * @param listener
     *            is a {@link Consumer} which is called for commit and rollback
     *            events.
     */
    public void addTransactionListener(final Consumer<Status> listener);

    /**
     * This method removes a transaction listener.
     * 
     * @param listener
     *            is the {@link Consumer} to be removed.
     */
    public void removeTransactionListener(final Consumer<Status> listener);

    /**
     * This method clears all transaction listeners.
     */
    public void clearTransactionListeners();
}
