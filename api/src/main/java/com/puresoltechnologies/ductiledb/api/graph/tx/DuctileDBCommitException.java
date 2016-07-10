package com.puresoltechnologies.ductiledb.api.graph.tx;

/**
 * This exception is thrown in cases of commit issues.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBCommitException extends DuctileDBTransactionException {

    private static final long serialVersionUID = 8129026234582637225L;

    public DuctileDBCommitException(Throwable cause) {
	super("Could not commit transaction.", cause);
    }
}
