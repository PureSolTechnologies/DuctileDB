package com.puresoltechnologies.ductiledb.api.schema;

/**
 * This exception is thrown in case a new property is added, but of property
 * with the same name is already defined.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBPropertyAlreadyDefinedException extends DuctileDBSchemaException {

    private static final long serialVersionUID = -3085383298750217517L;

    public DuctileDBPropertyAlreadyDefinedException(PropertyDefinition<?> definition) {
	super("Property with name '" + definition.getPropertyKey() + "' is aldready defined.");
    }

}
