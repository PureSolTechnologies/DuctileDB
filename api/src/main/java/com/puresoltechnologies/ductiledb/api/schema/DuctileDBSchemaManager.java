package com.puresoltechnologies.ductiledb.api.schema;

import java.io.Serializable;

import com.puresoltechnologies.ductiledb.api.ElementType;

/**
 * This interface contains the official API to handle DucileDB schema.
 * 
 * @author Rick-Rainer Ludwig
 */
public interface DuctileDBSchemaManager {

    /**
     * This method returns a list of names of all defined properties.
     * 
     * @return An {@link Iterable} is returned containing the property names.
     */
    public Iterable<String> getDefinedProperties();

    /**
     * This method is used to create a new property definition.
     * 
     * @param definition
     *            is a {@link PropertyDefinition} object defining the new
     *            property.
     */
    public <T extends Serializable> void defineProperty(PropertyDefinition<T> definition);

    /**
     * This method returns the property definition for a defined property.
     * 
     * @param propertyKey
     *            is the name of the property.
     * @return
     */
    public <T extends Serializable> PropertyDefinition<T> getPropertyDefinition(ElementType elementType,
	    String propertyKey);

    /**
     * This method removes a property definition.
     * 
     * @param propertyName
     */
    public void removePropertyDefinition(ElementType elementType, String propertyName);

}
