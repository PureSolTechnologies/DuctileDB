package com.puresoltechnologies.ductiledb.core.tables;

/**
 * This is the central interface for all statements which can be prepared with
 * place holders.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public interface PreparedStatement {

    /**
     * Takes the prepared statement and creates a bound statement out of it.
     * 
     * @return
     */
    public BoundStatement bind();

}
