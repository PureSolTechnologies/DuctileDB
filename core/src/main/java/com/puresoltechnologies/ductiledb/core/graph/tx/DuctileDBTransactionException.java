package com.puresoltechnologies.ductiledb.core.graph.tx;

import com.puresoltechnologies.ductiledb.core.DuctileDBException;

/**
 * This exception is thrown in cases of transaction issues.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class DuctileDBTransactionException extends DuctileDBException {

    private static final long serialVersionUID = 1797175288687956420L;

    public DuctileDBTransactionException(String message, Throwable cause) {
	super(message, cause);
    }

    public DuctileDBTransactionException(String message) {
	super(message);
    }

}
