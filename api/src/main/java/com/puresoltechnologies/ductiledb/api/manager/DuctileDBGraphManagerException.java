package com.puresoltechnologies.ductiledb.api.manager;

import com.puresoltechnologies.ductiledb.api.DuctileDBException;

/**
 * This exception is thrown by {@link DuctileDBGraphManager} in cases of
 * unexpected issues.
 * 
 * @author Rick-Rainer Ludwig
 *
 */
public class DuctileDBGraphManagerException extends DuctileDBException {

    private static final long serialVersionUID = -833508799331920625L;

    public DuctileDBGraphManagerException(String message, Throwable cause) {
	super(message, cause);
    }

    public DuctileDBGraphManagerException(String message) {
	super(message);
    }

}
