package com.puresoltechnologies.ductiledb.api.schema;

/**
 * This exception is thrown in case of a property which violates a unique
 * constraint.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBUniqueConstraintException extends DuctileDBConstraintException {

    private static final long serialVersionUID = -2387352754144564612L;

    public DuctileDBUniqueConstraintException(String message) {
	super(message);
    }

}
