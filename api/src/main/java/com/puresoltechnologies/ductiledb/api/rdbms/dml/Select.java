package com.puresoltechnologies.ductiledb.api.rdbms.dml;

/**
 * This interface is used for the SELECT implementation.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface Select {

    public TableRowIterable queryResults();

}
