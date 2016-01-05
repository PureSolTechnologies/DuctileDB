package com.puresoltechnologies.ductiledb.api;

import java.util.regex.Pattern;

/**
 * This exception is thrown in cases an invalid property key is used to add a
 * new property.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBInvalidPropertyKeyException extends DuctileDBSchemaException {

    private static final long serialVersionUID = 3591448580470653753L;

    public DuctileDBInvalidPropertyKeyException(String propertyKey, Pattern pattern) {
	super("Property key '" + propertyKey + "' does not match pattern '" + pattern + "' .");
    }

}
