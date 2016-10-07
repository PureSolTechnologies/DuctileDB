package com.puresoltechnologies.ductiledb.api.tables;

import com.puresoltechnologies.ductiledb.api.tables.dml.TableRowIterable;

/**
 * This is the base interface for all DuctileDB queries.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface DuctileDBQuery {

    public TableRowIterable execute();

}
