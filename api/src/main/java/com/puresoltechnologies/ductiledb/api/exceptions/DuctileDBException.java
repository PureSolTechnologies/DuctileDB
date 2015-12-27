package com.puresoltechnologies.ductiledb.api.exceptions;

/**
 * General purpose exception for all DuctileDB related issues. It is also used
 * as base class for more specific exceptions.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBException extends RuntimeException {

    private static final long serialVersionUID = -2552842856696970076L;

    public DuctileDBException(String message, Throwable cause) {
	super(message, cause);
    }

    public DuctileDBException(String message) {
	super(message);
    }

}
