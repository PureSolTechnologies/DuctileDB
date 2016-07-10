package com.puresoltechnologies.ductiledb.api.graph.schema;

import java.io.Serializable;
import java.util.Set;

import com.puresoltechnologies.ductiledb.api.graph.ElementType;

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
     * @param <T>
     *            is the actual type of the property.
     */
    public <T extends Serializable> void defineProperty(PropertyDefinition<T> definition);

    /**
     * This method returns the property definition for a defined property.
     * 
     * @param elementType
     *            is the type of the element defined via {@link ElementType}.
     * @param propertyKey
     *            is the name of the property.
     * @param <T>
     *            is the actual type of the property.
     * @return A {@link PropertyDefinition} is returned.
     */
    public <T extends Serializable> PropertyDefinition<T> getPropertyDefinition(ElementType elementType,
	    String propertyKey);

    /**
     * This method removes a property definition.
     * 
     * @param elementType
     *            is the type of the element provided via enumeration
     *            {@link ElementType}.
     * @param propertyKey
     *            is the name of property for which the definition is to be
     *            removed.
     */
    public void removePropertyDefinition(ElementType elementType, String propertyKey);

    /**
     * This method returns names of all defined types.
     * 
     * @return An {@link Iterable} of {@link String} is returned containing
     *         names of all defined types.
     */
    public Iterable<String> getDefinedTypes();

    /**
     * This method defines a new type for an element type.
     * 
     * @param elementType
     *            is the type of the graph element for which the type can be
     *            applied.
     * @param typeName
     *            is the name of the new type.
     * @param propertyKeys
     *            is a {@link Set} of property names, which are applied to the
     *            type.
     */
    public void defineType(ElementType elementType, String typeName, Set<String> propertyKeys);

    /**
     * This method reads a type definition and returns the names of all
     * properties which are assigned to it.
     * 
     * @param elementType
     *            is the type of the graph element for which the type can be
     *            applied.
     * @param typeName
     *            is the name of the new type.
     * @return A {@link Set} of {@link String} is returned containing the names
     *         of the properties which are assigned to the type.
     */
    public Set<String> getTypeDefinition(ElementType elementType, String typeName);

    /**
     * This method removes a type.
     * 
     * @param elementType
     *            is the type of the graph element for which the type can be
     *            applied.
     * @param typeName
     *            is the name of the new type.
     */
    public void removeTypeDefinition(ElementType elementType, String typeName);
}
