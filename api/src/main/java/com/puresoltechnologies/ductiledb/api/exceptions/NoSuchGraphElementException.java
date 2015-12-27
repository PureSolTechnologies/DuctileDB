package com.puresoltechnologies.ductiledb.api.exceptions;

/**
 * This exception is thrown in cases of access to non-existing graph elements.
 * 
 * @author Rick-Rainer Ludwig
 */
public class NoSuchGraphElementException extends DuctileDBException {

    private static final long serialVersionUID = 5458339406152551681L;

    public NoSuchGraphElementException(String message, Throwable cause) {
	super(message, cause);
    }

    public NoSuchGraphElementException(String message) {
	super(message);
    }

}
