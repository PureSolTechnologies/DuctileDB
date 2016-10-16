package com.puresoltechnologies.ductiledb.core.tables;

import com.puresoltechnologies.ductiledb.core.tables.dml.TableRowIterable;

/**
 * This is the base class for DuctileDB statements.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface Statement {

    public TableRowIterable execute() throws ExecutionException;

}
