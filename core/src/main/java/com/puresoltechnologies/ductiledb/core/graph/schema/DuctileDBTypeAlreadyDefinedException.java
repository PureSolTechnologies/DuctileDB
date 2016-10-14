package com.puresoltechnologies.ductiledb.core.graph.schema;

/**
 * This exception is thrown in case a new type is added, but of type with the
 * same name is already defined.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBTypeAlreadyDefinedException extends DuctileDBSchemaException {

    private static final long serialVersionUID = -3094844733214328734L;

    public DuctileDBTypeAlreadyDefinedException(String typeName) {
	super("Type with name '" + typeName + "' is aldready defined.");
    }

}
