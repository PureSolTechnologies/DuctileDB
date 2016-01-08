package com.puresoltechnologies.ductiledb.core.schema;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.puresoltechnologies.ductiledb.api.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.api.ElementType;
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

    private final Map<ElementType, Map<String, PropertyDefinition<?>>> propertyDefinitions = new HashMap<>();
    private final Map<ElementType, Map<String, Set<String>>> typeDefinitions = new HashMap<>();

    private final DuctileDBGraphImpl graph;

    public DuctileDBSchema(DuctileDBGraphImpl graph) {
	super();
	this.graph = graph;
	readSchemaInformation();
    }

    private void readSchemaInformation() {
	readPropertyDefinitions();
	readTypeDefinitions();
    }

    private void readPropertyDefinitions() {
	propertyDefinitions.put(ElementType.VERTEX, new HashMap<>());
	propertyDefinitions.put(ElementType.EDGE, new HashMap<>());
	DuctileDBSchemaManager graphManager = graph.createSchemaManager();
	for (String propertyKey : graphManager.getDefinedProperties()) {
	    for (ElementType elementType : ElementType.values()) {
		PropertyDefinition<Serializable> propertyDefinition = graphManager.getPropertyDefinition(elementType,
			propertyKey);
		if (propertyDefinition != null) {
		    defineProperty(propertyDefinition);
		}
	    }
	}
    }

    private void readTypeDefinitions() {
	typeDefinitions.put(ElementType.VERTEX, new HashMap<>());
	typeDefinitions.put(ElementType.EDGE, new HashMap<>());
	DuctileDBSchemaManager graphManager = graph.createSchemaManager();
	for (String typeName : graphManager.getDefinedTypes()) {
	    for (ElementType elementType : ElementType.values()) {
		Set<String> typeProperties = graphManager.getTypeDefinition(elementType, typeName);
		if (typeProperties != null) {
		    defineType(elementType, typeName, typeProperties);
		}
	    }
	}
    }

    public void defineProperty(PropertyDefinition<?> definition) {
	Map<String, PropertyDefinition<?>> elementPropertyDefinitions = propertyDefinitions
		.get(definition.getElementType());
	elementPropertyDefinitions.put(definition.getPropertyKey(), definition);
    }

    public void removeProperty(ElementType elementType, String propertyKey) {
	propertyDefinitions.get(elementType).remove(propertyKey);
    }

    public void checkAddVertex(Set<String> types, Map<String, Object> properties) {
	types.forEach((type) -> {
	    checkTypeIdentifier(type);
	});
	properties.forEach((key, value) -> {
	    checkPropertyIdentifier(key);
	    PropertyDefinition<?> definition = propertyDefinitions.get(ElementType.VERTEX).get(key);
	    if (definition != null) {
		checkPropertyType(value, definition);
		checkGlobalConstraint(ElementType.VERTEX, key, value, definition);
		types.forEach((type) -> {
		    checkTypeConstraint(ElementType.VERTEX, type, key, value, definition);
		});
	    }
	});
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

    public void checkAddEdge(String type, Map<String, Object> properties) {
	checkTypeIdentifier(type);
	properties.forEach((key, value) -> {
	    checkPropertyIdentifier(key);
	    PropertyDefinition<?> definition = propertyDefinitions.get(ElementType.EDGE).get(key);
	    if (definition != null) {
		checkPropertyType(value, definition);
		checkGlobalConstraint(ElementType.EDGE, key, value, definition);
		checkTypeConstraint(ElementType.EDGE, type, key, value, definition);
	    }
	});
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

    public void checkAddVertexType(String type, Map<String, Object> properties) {
	checkTypeIdentifier(type);
    }

    private void checkTypeIdentifier(String type) {
	if (!IDENTIFIER_PATTERN.matcher(type).matches()) {
	    throw new DuctileDBInvalidTypeNameException(type, IDENTIFIER_PATTERN);
	}
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
    public void checkSetVertexProperty(Set<String> types, String key, Object value) {
	PropertyDefinition<?> definition = propertyDefinitions.get(ElementType.VERTEX).get(key);
	checkPropertyIdentifier(key);
	if (definition != null) {
	    checkPropertyType(value, definition);
	    checkGlobalConstraint(ElementType.VERTEX, key, value, definition);
	}
    }

    public void checkSetEdgeProperty(String type, String key, Object value) {
	PropertyDefinition<?> definition = propertyDefinitions.get(ElementType.EDGE).get(key);
	checkPropertyIdentifier(key);
	if (definition != null) {
	    checkPropertyType(value, definition);
	    checkGlobalConstraint(ElementType.EDGE, key, value, definition);
	}
    }

    private void checkPropertyIdentifier(String key) {
	if (!IDENTIFIER_PATTERN.matcher(key).matches()) {
	    throw new DuctileDBInvalidPropertyKeyException(key, IDENTIFIER_PATTERN);
	}
    }

    private void checkPropertyType(Object value, PropertyDefinition<?> definition) {
	if (!value.getClass().equals(definition.getPropertyType())) {
	    throw new DuctileDBInvalidPropertyTypeException(value.getClass(), definition.getPropertyType());
	}
    }

    private void checkGlobalConstraint(ElementType elementType, String key, Object value,
	    PropertyDefinition<?> definition) {
	if (definition.getUniqueConstraint() == UniqueConstraint.GLOBAL) {
	    if (elementType == ElementType.VERTEX) {
		Iterable<DuctileDBVertex> vertices = graph.getVertices(key, value);
		if (vertices.iterator().hasNext()) {
		    throw new DuctileDBUniqueConstraintViolationException(UniqueConstraint.GLOBAL, key, value);
		}
	    } else if (elementType == ElementType.EDGE) {
		Iterable<DuctileDBEdge> edges = graph.getEdges(key, value);
		if (edges.iterator().hasNext()) {
		    throw new DuctileDBUniqueConstraintViolationException(UniqueConstraint.GLOBAL, key, value);
		}
	    }
	}
    }

    private void checkTypeConstraint(ElementType elementType, String type, String key, Object value,
	    PropertyDefinition<?> definition) {
	if (definition.getUniqueConstraint() == UniqueConstraint.TYPE) {
	    if (elementType == ElementType.VERTEX) {
		Iterable<DuctileDBVertex> vertices = graph.getVertices(key, value);
		for (DuctileDBVertex vertex : vertices) {
		    for (String vertexType : vertex.getTypes()) {
			if (vertexType.equals(type)) {
			    throw new DuctileDBUniqueConstraintViolationException(UniqueConstraint.TYPE, key, value);
			}
		    }
		}
	    } else if (elementType == ElementType.EDGE) {
		Iterable<DuctileDBEdge> edges = graph.getEdges(key, value);
		for (DuctileDBEdge edge : edges) {
		    if (edge.getType().equals(type)) {
			throw new DuctileDBUniqueConstraintViolationException(UniqueConstraint.TYPE, key, value);
		    }
		}
	    }
	}
    }

    public void defineType(ElementType elementType, String typeName, Set<String> propertyKeys) {
	typeDefinitions.get(elementType).put(typeName, propertyKeys);
    }

    public void removeType(ElementType elementType, String typeName) {
	typeDefinitions.get(elementType).remove(typeName);
    }

}
