package com.puresoltechnologies.ductiledb.api.graph.schema;

/**
 * This exception is thrown in cases an invalid property type is used to add a
 * new property.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBInvalidPropertyTypeException extends DuctileDBSchemaException {

    private static final long serialVersionUID = 3591448580470653753L;

    public DuctileDBInvalidPropertyTypeException(Class<?> propertyType, Class<?> expectedType) {
	super("Property type '" + propertyType.getName() + "' does not match expected type '" + expectedType.getName()
		+ "'.");
    }

}
