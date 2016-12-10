package com.puresoltechnologies.ductiledb.core.tables.dml;

import com.puresoltechnologies.ductiledb.core.tables.PreparedStatement;

/**
 * This is the central interface for all statements which can be prepared with
 * place holders.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public interface PreparedDMLStatement extends PreparedStatement {

    /**
     * This method adds a new placeholder.
     * 
     * @param placeholder
     */
    public PreparedDMLStatement addPlaceholder(Placeholder placeholder);
}
