package com.puresoltechnologies.ductiledb.core.tables;

import com.puresoltechnologies.ductiledb.core.tables.dml.TableRowIterable;

/**
 * This is the base interface for all DuctileDB queries.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface DuctileDBQuery {

    public TableRowIterable execute();

}
