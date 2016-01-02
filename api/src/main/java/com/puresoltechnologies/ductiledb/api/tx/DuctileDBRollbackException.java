package com.puresoltechnologies.ductiledb.api.tx;

/**
 * This exception is thrown in cases of rollback issues.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBRollbackException extends DuctileDBTransactionException {

    private static final long serialVersionUID = 6722786685415668005L;

    public DuctileDBRollbackException(Throwable cause) {
	super("Could not rollback transaction.", cause);
    }

}
