package com.puresoltechnologies.ductiledb.core.graph.schema;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import com.puresoltechnologies.ductiledb.core.graph.DuctileDBEdge;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBElement;
import com.puresoltechnologies.ductiledb.core.graph.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.core.graph.ElementType;
import com.puresoltechnologies.ductiledb.core.graph.GraphStoreImpl;
import com.puresoltechnologies.ductiledb.core.graph.manager.DuctileDBGraphManager;
import com.puresoltechnologies.ductiledb.core.graph.utils.ElementUtils;

/**
 * This method reads all schema information from {@link DuctileDBGraphManager},
 * stores its information and handles all checks for schema validity.
 * 
 * @author Rick-Rainer Ludwig
 */
public class DuctileDBSchema {

    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("[a-zA-Z0-9][-._a-zA-Z0-9]*");

    private final Map<ElementType, Map<String, PropertyDefinition<?>>> propertyDefinitions = new HashMap<>();
    private final Map<ElementType, Map<String, Set<String>>> typeDefinitions = new ConcurrentHashMap<>();

    private final GraphStoreImpl graph;

    public DuctileDBSchema(GraphStoreImpl graph) {
	super();
	this.graph = graph;
	readSchemaInformation();
    }

    private void readSchemaInformation() {
	readPropertyDefinitions();
	readTypeDefinitions();
    }

    private void readPropertyDefinitions() {
	propertyDefinitions.put(ElementType.VERTEX, new ConcurrentHashMap<>());
	propertyDefinitions.put(ElementType.EDGE, new ConcurrentHashMap<>());
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
		checkGlobalConstraint(null, ElementType.VERTEX, key, value, definition);
		types.forEach((type) -> {
		    checkTypeConstraint(null, ElementType.VERTEX, type, key, value, definition);
		});
	    }
	});
    }

    /**
     * This method checks the given type for DuctileDB and also schema
     * conformance.
     * 
     * @param type
     *            is the type of the edge.
     * @param properties
     *            are the properties to be added as defined with the {@link Map}
     *            .
     */

    public void checkAddEdge(String type, Map<String, Object> properties) {
	checkTypeIdentifier(type);
	properties.forEach((key, value) -> {
	    checkPropertyIdentifier(key);
	    PropertyDefinition<?> definition = propertyDefinitions.get(ElementType.EDGE).get(key);
	    if (definition != null) {
		checkPropertyType(value, definition);
		checkGlobalConstraint(null, ElementType.EDGE, key, value, definition);
		checkTypeConstraint(null, ElementType.EDGE, type, key, value, definition);
	    }
	});
    }

    /**
     * This method checks the given type for DuctileDB and also schema
     * conformance.
     * 
     * @param vertex
     *            is the vertex to add the new type to.
     * @param type
     *            is the type of the vertex.
     */
    public void checkAddVertexType(DuctileDBVertex vertex, String type) {
	checkTypeIdentifier(type);
	// checkTypeProperties(ElementType.VERTEX, type, properties);
    }

    private void checkTypeIdentifier(String type) {
	if (!IDENTIFIER_PATTERN.matcher(type).matches()) {
	    throw new DuctileDBInvalidTypeNameException(type, IDENTIFIER_PATTERN);
	}
    }

    private void checkTypeProperties(ElementType elementType, String type, Map<String, Object> properties) {
	Set<String> typeProperties = typeDefinitions.get(elementType).get(type);
	if (typeProperties == null) {
	    return;
	}
	Set<String> propertyKeys = properties.keySet();
	for (String typePropertyKey : typeProperties) {
	    if (!propertyKeys.contains(typePropertyKey)) {
		throw new DuctileDBSchemaException(
			"Property '" + typePropertyKey + "' is missing for type '" + type + "'.");
	    }
	}
    }

    /**
     * This method checks the given property for DuctileDB and also schema
     * conformance.
     * 
     * @param vertex
     *            is the {@link DuctileDBVertex} to check the property for.
     * @param key
     *            is the name of the property.
     * @param value
     *            is the value of the property.
     */
    public void checkSetVertexProperty(DuctileDBVertex vertex, String key, Object value) {
	PropertyDefinition<?> definition = propertyDefinitions.get(ElementType.VERTEX).get(key);
	checkPropertyIdentifier(key);
	if (definition != null) {
	    checkPropertyType(value, definition);
	    checkGlobalConstraint(vertex, ElementType.VERTEX, key, value, definition);
	    vertex.getTypes().forEach((type) -> {
		checkTypeConstraint(vertex, ElementType.VERTEX, type, key, value, definition);
	    });
	}
    }

    public void checkSetEdgeProperty(DuctileDBEdge edge, String key, Object value) {
	PropertyDefinition<?> definition = propertyDefinitions.get(ElementType.EDGE).get(key);
	checkPropertyIdentifier(key);
	if (definition != null) {
	    checkPropertyType(value, definition);
	    checkGlobalConstraint(edge, ElementType.EDGE, key, value, definition);
	    checkTypeConstraint(edge, ElementType.EDGE, edge.getType(), key, value, definition);
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

    private void checkGlobalConstraint(DuctileDBElement element, ElementType elementType, String key, Object value,
	    PropertyDefinition<?> definition) {
	if (definition.getUniqueConstraint() == UniqueConstraint.GLOBAL) {
	    long id = element != null ? element.getId() : -1;
	    if (elementType == ElementType.VERTEX) {
		Iterable<DuctileDBVertex> vertices = graph.getVertices(key, value);
		Iterator<DuctileDBVertex> iterator = vertices.iterator();
		while (iterator.hasNext()) {
		    if (id != iterator.next().getId()) {
			throw new DuctileDBUniqueConstraintViolationException(UniqueConstraint.GLOBAL, key, value);
		    }
		}
	    } else if (elementType == ElementType.EDGE) {
		Iterable<DuctileDBEdge> edges = graph.getEdges(key, value);
		Iterator<DuctileDBEdge> iterator = edges.iterator();
		while (iterator.hasNext()) {
		    if (id != iterator.next().getId()) {
			throw new DuctileDBUniqueConstraintViolationException(UniqueConstraint.GLOBAL, key, value);
		    }
		}
	    }
	}
    }

    private void checkTypeConstraint(DuctileDBElement element, ElementType elementType, String type, String key,
	    Object value, PropertyDefinition<?> definition) {
	if (definition.getUniqueConstraint() == UniqueConstraint.TYPE) {
	    long id = element != null ? element.getId() : -1;
	    if (elementType == ElementType.VERTEX) {
		Iterable<DuctileDBVertex> vertices = graph.getVertices(key, value);
		for (DuctileDBVertex vertex : vertices) {
		    if (id != vertex.getId()) {
			for (String vertexType : vertex.getTypes()) {
			    if (vertexType.equals(type)) {
				throw new DuctileDBUniqueConstraintViolationException(UniqueConstraint.TYPE, key,
					value);
			    }
			}
		    }
		}
	    } else if (elementType == ElementType.EDGE) {
		Iterable<DuctileDBEdge> edges = graph.getEdges(key, value);
		for (DuctileDBEdge edge : edges) {
		    if ((id != edge.getId()) && (edge.getType().equals(type))) {
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

    public void checkRemoveVertexProperty(DuctileDBVertex vertex, String propertyKey) {
	vertex.getTypes().forEach(type -> checkRemoveProperty(ElementType.VERTEX, type, propertyKey));
    }

    public void checkRemoveEdgeProperty(DuctileDBEdge edge, String propertyKey) {
	checkRemoveProperty(ElementType.EDGE, edge.getType(), propertyKey);
    }

    private void checkRemoveProperty(ElementType elementType, String type, String propertyKey) {
	Set<String> typeDefinition = typeDefinitions.get(elementType).get(type);
	if (typeDefinition == null) {
	    return;
	}
	if (typeDefinition.contains(propertyKey)) {
	    throw new DuctileDBSchemaException(
		    "Property '" + propertyKey + "' cannot be removed, because type '" + type + "' uses it.");
	}
    }

    public void checkVertex(DuctileDBVertex vertex) {
	Map<String, Object> properties = ElementUtils.getProperties(vertex);
	vertex.getTypes().forEach((type) -> {
	    checkTypeIdentifier(type);
	    checkTypeProperties(ElementType.VERTEX, type, properties);
	});
	properties.forEach((key, value) -> {
	    checkPropertyIdentifier(key);
	    PropertyDefinition<?> definition = propertyDefinitions.get(ElementType.VERTEX).get(key);
	    if (definition != null) {
		checkPropertyType(value, definition);
		checkGlobalConstraint(vertex, ElementType.VERTEX, key, value, definition);
		vertex.getTypes().forEach((type) -> {
		    checkTypeConstraint(vertex, ElementType.VERTEX, type, key, value, definition);
		});
	    }
	});
    }

    public void checkEdge(DuctileDBEdge edge) {
	Map<String, Object> properties = ElementUtils.getProperties(edge);
	checkTypeIdentifier(edge.getType());
	checkTypeProperties(ElementType.EDGE, edge.getType(), properties);
	properties.forEach((key, value) -> {
	    checkPropertyIdentifier(key);
	    PropertyDefinition<?> definition = propertyDefinitions.get(ElementType.EDGE).get(key);
	    if (definition != null) {
		checkPropertyType(value, definition);
		checkGlobalConstraint(edge, ElementType.EDGE, key, value, definition);
		checkTypeConstraint(edge, ElementType.EDGE, edge.getType(), key, value, definition);
	    }
	});
    }

}
