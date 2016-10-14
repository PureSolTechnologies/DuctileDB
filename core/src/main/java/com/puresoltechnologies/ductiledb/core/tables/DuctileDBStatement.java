package com.puresoltechnologies.ductiledb.core.tables;

/**
 * This is the base class for DuctileDB statements.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface DuctileDBStatement {

    public void execute() throws ExecutionException;

}
