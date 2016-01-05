package com.puresoltechnologies.ductiledb.api;

import java.util.regex.Pattern;

/**
 * This exception is thrown in cases an invalid type name is used to add a new
 * type.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBInvalidTypeNameException extends DuctileDBSchemaException {

    private static final long serialVersionUID = 3591448580470653753L;

    public DuctileDBInvalidTypeNameException(String propertyKey, Pattern pattern) {
	super("Type name '" + propertyKey + "' does not match pattern '" + pattern + "' .");
    }

}
