package com.puresoltechnologies.ductiledb.core.schema;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.api.manager.DuctileDBGraphManager;
import com.puresoltechnologies.ductiledb.api.schema.DuctileDBInvalidPropertyKeyException;
import com.puresoltechnologies.ductiledb.api.schema.DuctileDBInvalidPropertyTypeException;
import com.puresoltechnologies.ductiledb.api.schema.DuctileDBInvalidTypeNameException;
import com.puresoltechnologies.ductiledb.api.schema.DuctileDBSchemaManager;
import com.puresoltechnologies.ductiledb.api.schema.DuctileDBUniqueConstraintViolationException;
import com.puresoltechnologies.ductiledb.api.schema.PropertyDefinition;
import com.puresoltechnologies.ductiledb.api.schema.UniqueConstraint;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphImpl;

/**
 * This method reads all schema information from {@link DuctileDBGraphManager},
 * stores its information and handles all checks for schema validity.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBSchema {

    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("[a-zA-Z0-9][-._a-zA-Z0-9]*");

    private final Map<String, PropertyDefinition<?>> propertyDefinitions = new HashMap<>();

    private final DuctileDBGraphImpl graph;

    public DuctileDBSchema(DuctileDBGraphImpl graph) {
	super();
	this.graph = graph;
	readSchemaInformation();
    }

    private void readSchemaInformation() {
	DuctileDBSchemaManager graphManager = graph.createSchemaManager();
	Iterable<String> definedProperties = graphManager.getDefinedProperties();
	for (String propertyKey : definedProperties) {
	    PropertyDefinition<Serializable> propertyDefinition = graphManager.getPropertyDefinition(propertyKey);
	    defineProperty(propertyDefinition);
	}
    }

    public void checkTypes(Set<String> types) {
	types.forEach(type -> checkType(type));
    }

    /**
     * This method checks the given type for DuctileDB and also schema
     * conformance.
     * 
     * @param key
     *            is the name of the property.
     * @param value
     *            is the value of the property.
     */
    public void checkType(String type) {
	if (!IDENTIFIER_PATTERN.matcher(type).matches()) {
	    throw new DuctileDBInvalidTypeNameException(type, IDENTIFIER_PATTERN);
	}
    }

    public void checkProperties(Map<String, Object> properties) {
	properties.forEach((key, value) -> checkProperty(key, value));
    }

    /**
     * This method checks the given property for DuctileDB and also schema
     * conformance.
     * 
     * @param key
     *            is the name of the property.
     * @param value
     *            is the value of the property.
     */
    public void checkProperty(String key, Object value) {
	if (!IDENTIFIER_PATTERN.matcher(key).matches()) {
	    throw new DuctileDBInvalidPropertyKeyException(key, IDENTIFIER_PATTERN);
	}
	PropertyDefinition<?> definition = propertyDefinitions.get(key);
	if (definition != null) {
	    if (!value.getClass().equals(definition.getPropertyType())) {
		throw new DuctileDBInvalidPropertyTypeException(value.getClass(), definition.getPropertyType());
	    }
	    if (definition.getUniqueConstraint() == UniqueConstraint.GLOBAL) {
		Iterable<DuctileDBVertex> vertices = graph.getVertices(key, value);
		if (vertices.iterator().hasNext()) {
		    throw new DuctileDBUniqueConstraintViolationException(UniqueConstraint.GLOBAL, key, value);
		}
	    }
	}
    }

    public void defineProperty(PropertyDefinition<?> definition) {
	propertyDefinitions.put(definition.getPropertyKey(), definition);
    }

    public void removeProperty(String propertyKey) {
	propertyDefinitions.remove(propertyKey);
    }

}
