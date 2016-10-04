package com.puresoltechnologies.ductiledb.api.rdbms;

import com.puresoltechnologies.ductiledb.api.rdbms.dml.TableRowIterable;

/**
 * This is the base interface for all DuctileDB queries.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface DuctileDBQuery {

    public TableRowIterable execute();

}
