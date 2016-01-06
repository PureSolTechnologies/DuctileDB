package com.puresoltechnologies.ductiledb.api.schema;

/**
 * This exception is thrown in case of a property which violates a unique
 * constraint.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBUniqueConstraintViolationException extends DuctileDBConstraintException {

    private static final long serialVersionUID = -2387352754144564612L;

    public DuctileDBUniqueConstraintViolationException(UniqueConstraint constraint, String key, Object value) {
	super("A '" + constraint.name() + "' for property '" + key + "' was defined, but a value '" + value
		+ "' was already assigned before.");
    }

}
