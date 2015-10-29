package com.puresoltechnologies.ductiledb;

/**
 * General purpose exception for all HGraph related issues.
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
