package com.puresoltechnologies.ductiledb.core.tx;

import java.io.IOException;

/**
 * This interface represents a single operation which is to be performed during
 * within a transaction.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface TxOperation {

    /**
     * This method is called to perform the actual transformation in the store.
     * 
     * @throws IOException
     *             is thrown in case something is wrong with the underlying
     *             store.
     */
    void perform() throws IOException;

    /**
     * This method is used to "commit" the changes into the transaction buffer.
     */
    void commitInternally();

    /**
     * This method is used to "rollback" the changes in the transaction buffer.
     */
    void rollbackInternally();
}
