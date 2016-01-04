package com.puresoltechnologies.ductiledb.api;

/**
 * This exception is thrown in cases of schema violations.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBSchemaException extends DuctileDBException {

    private static final long serialVersionUID = 1L;

    public DuctileDBSchemaException(String message, Throwable cause) {
	super(message, cause);
    }

    public DuctileDBSchemaException(String message) {
	super(message);
    }

}
