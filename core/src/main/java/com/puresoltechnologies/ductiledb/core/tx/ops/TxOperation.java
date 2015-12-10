package com.puresoltechnologies.ductiledb.core.tx.ops;

import java.io.IOException;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;

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

    DuctileDBVertex updateVertex(DuctileDBVertex vertex);

    DuctileDBEdge updateEdge(DuctileDBEdge edge);

}
