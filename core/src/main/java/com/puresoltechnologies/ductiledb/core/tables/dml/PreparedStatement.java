package com.puresoltechnologies.ductiledb.core.tables.dml;

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
     * @return A {@link BoundStatement} is returned which can be used to be
     *         executed.
     */
    public BoundStatement bind();

    /**
     * Takes the prepared statement and creates a bound statement out of it.
     * 
     * @param values
     *            is an array of values to be assigned to the defined
     *            placeholders. The assignment starts from 1 to the end
     *            successively.
     * @return A {@link BoundStatement} is returned which can be used to be
     *         executed.
     */
    public BoundStatement bind(Object... values);

}
