package com.puresoltechnologies.ductiledb.api.graph.schema;

/**
 * This exception is used to signal general constraint issues during operation.
 * Specific constrain exception should extend this exception.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBConstraintException extends DuctileDBSchemaException {

    private static final long serialVersionUID = -2105669455021490034L;

    public DuctileDBConstraintException(String message, Throwable cause) {
	super(message, cause);
    }

    public DuctileDBConstraintException(String message) {
	super(message);
    }

}
