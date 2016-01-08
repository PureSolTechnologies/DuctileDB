package com.puresoltechnologies.ductiledb.core.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.api.DuctileDBVertex;
import com.puresoltechnologies.ductiledb.api.ElementType;
import com.puresoltechnologies.ductiledb.api.schema.DuctileDBInvalidPropertyTypeException;
import com.puresoltechnologies.ductiledb.api.schema.DuctileDBPropertyAlreadyDefinedException;
import com.puresoltechnologies.ductiledb.api.schema.DuctileDBSchemaException;
import com.puresoltechnologies.ductiledb.api.schema.DuctileDBSchemaManager;
import com.puresoltechnologies.ductiledb.api.schema.DuctileDBUniqueConstraintViolationException;
import com.puresoltechnologies.ductiledb.api.schema.PropertyDefinition;
import com.puresoltechnologies.ductiledb.api.schema.UniqueConstraint;
import com.puresoltechnologies.ductiledb.core.AbstractDuctileDBGraphTest;
import com.puresoltechnologies.ductiledb.core.DuctileDBTestHelper;

public class DuctileDBSchemaManagerIT extends AbstractDuctileDBGraphTest {

    private static DuctileDBGraph graph;
    private static DuctileDBSchemaManager schemaManager;

    @BeforeClass
    public static void initialize() {
	graph = getGraph();
	schemaManager = graph.createSchemaManager();
	assertNotNull("Schema manager was not provided.", schemaManager);
    }

    @Before
    public void cleanup() throws IOException {
	DuctileDBTestHelper.removeGraph(graph);
    }

    @Test
    public void testInitialPropertyDefinitions() {
	Iterable<String> definedProperties = schemaManager.getDefinedProperties();
	assertNotNull(definedProperties);
	assertFalse(definedProperties.iterator().hasNext());
    }

    @Test
    public void testAddGetAndRemovePropertyDefinitions() {
	assertNull(schemaManager.getPropertyDefinition(ElementType.VERTEX, "property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.VERTEX, "property", String.class,
		UniqueConstraint.GLOBAL);
	schemaManager.defineProperty(definition);
	PropertyDefinition<String> readDefinition = schemaManager.getPropertyDefinition(ElementType.VERTEX, "property");
	assertNotNull(readDefinition);
	assertEquals(definition, readDefinition);
	schemaManager.removePropertyDefinition(ElementType.VERTEX, "property");
	assertNull(schemaManager.getPropertyDefinition(ElementType.VERTEX, "property"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDefinePropertyWithNullThrowsIllegalArgumentException() {
	schemaManager.defineProperty(null);
    }

    @Test(expected = DuctileDBPropertyAlreadyDefinedException.class)
    public void testDoubleAddedPropertyLeadsToException() {
	assertNull(schemaManager.getPropertyDefinition(ElementType.VERTEX, "property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.VERTEX, "property", String.class,
		UniqueConstraint.GLOBAL);
	schemaManager.defineProperty(definition);
	PropertyDefinition<String> readDefinition = schemaManager.getPropertyDefinition(ElementType.VERTEX, "property");
	assertNotNull(readDefinition);
	assertEquals(definition, readDefinition);
	schemaManager.defineProperty(definition);
    }

    @Test
    public void testCorrectPropertyType() {
	assertNull(schemaManager.getPropertyDefinition(ElementType.VERTEX, "property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.VERTEX, "property", String.class,
		UniqueConstraint.NONE);
	schemaManager.defineProperty(definition);

	Set<String> types = new HashSet<>();
	types.add("type");
	Map<String, Object> properties = new HashMap<>();
	properties.put("property", "StringValue");
	try {
	    graph.addVertex(types, properties);
	} finally {
	    graph.rollback();
	}
    }

    @Test(expected = DuctileDBInvalidPropertyTypeException.class)
    public void testIncorrectPropertyType() {
	assertNull(schemaManager.getPropertyDefinition(ElementType.VERTEX, "property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.VERTEX, "property", String.class,
		UniqueConstraint.NONE);
	schemaManager.defineProperty(definition);

	Set<String> types = new HashSet<>();
	types.add("type");
	Map<String, Object> properties = new HashMap<>();
	properties.put("property", 1l);
	try {
	    graph.addVertex(types, properties);
	} finally {
	    graph.rollback();
	}
    }

    @Test(expected = DuctileDBInvalidPropertyTypeException.class)
    public void testIncorrectPropertyType2() {
	assertNull(schemaManager.getPropertyDefinition(ElementType.VERTEX, "property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.VERTEX, "property", String.class,
		UniqueConstraint.NONE);
	schemaManager.defineProperty(definition);

	Set<String> types = new HashSet<>();
	types.add("type");
	try {
	    DuctileDBVertex vertex = graph.addVertex(types, new HashMap<>());
	    vertex.setProperty("property", 1l);
	} finally {
	    graph.rollback();
	}
    }

    @Test(expected = DuctileDBUniqueConstraintViolationException.class)
    public void testGlobalVertexUniqueConstraint() {
	assertNull(schemaManager.getPropertyDefinition(ElementType.VERTEX, "property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.VERTEX, "property", String.class,
		UniqueConstraint.GLOBAL);
	schemaManager.defineProperty(definition);

	Set<String> types1 = new HashSet<>();
	types1.add("type1");
	Set<String> types2 = new HashSet<>();
	types2.add("type2");
	Map<String, Object> properties = new HashMap<>();
	properties.put("property", "value");
	try {
	    graph.addVertex(types1, properties);
	    graph.addVertex(types2, properties);
	} finally {
	    graph.rollback();
	}
    }

    @Test
    public void testVertexTypeUniqueConstraintValid() {
	assertNull(schemaManager.getPropertyDefinition(ElementType.VERTEX, "property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.VERTEX, "property", String.class,
		UniqueConstraint.TYPE);
	schemaManager.defineProperty(definition);

	Set<String> types1 = new HashSet<>();
	types1.add("type1");
	Set<String> types2 = new HashSet<>();
	types2.add("type2");
	Map<String, Object> properties = new HashMap<>();
	properties.put("property", "value");
	try {
	    graph.addVertex(types1, properties);
	    graph.addVertex(types2, properties);
	} finally {
	    graph.rollback();
	}
    }

    @Test(expected = DuctileDBUniqueConstraintViolationException.class)
    public void testVertexTypeUniqueConstraintViolation() {
	assertNull(schemaManager.getPropertyDefinition(ElementType.VERTEX, "property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.VERTEX, "property", String.class,
		UniqueConstraint.TYPE);
	schemaManager.defineProperty(definition);

	Set<String> types1 = new HashSet<>();
	types1.add("type1");
	Set<String> types2 = new HashSet<>();
	types2.add("type1");
	types2.add("type2");
	Map<String, Object> properties = new HashMap<>();
	properties.put("property", "value");
	try {
	    graph.addVertex(types1, properties);
	    graph.addVertex(types2, properties);
	} finally {
	    graph.rollback();
	}
    }

    @Test(expected = DuctileDBUniqueConstraintViolationException.class)
    public void testGlobalEdgeUniqueConstraint() {
	assertNull(schemaManager.getPropertyDefinition(ElementType.EDGE, "property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.EDGE, "property", String.class,
		UniqueConstraint.GLOBAL);
	schemaManager.defineProperty(definition);

	Map<String, Object> properties = new HashMap<>();
	properties.put("property", "value");
	try {
	    DuctileDBVertex vertex1 = graph.addVertex();
	    DuctileDBVertex vertex2 = graph.addVertex();
	    DuctileDBVertex vertex3 = graph.addVertex();
	    vertex1.addEdge("type1", vertex2, properties);
	    vertex2.addEdge("type2", vertex3, properties);
	} finally {
	    graph.rollback();
	}
    }

    @Test
    public void testEdgeTypeUniqueConstraintValid() {
	assertNull(schemaManager.getPropertyDefinition(ElementType.EDGE, "property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.EDGE, "property", String.class,
		UniqueConstraint.TYPE);
	schemaManager.defineProperty(definition);

	Map<String, Object> properties = new HashMap<>();
	properties.put("property", "value");
	try {
	    DuctileDBVertex vertex1 = graph.addVertex();
	    DuctileDBVertex vertex2 = graph.addVertex();
	    DuctileDBVertex vertex3 = graph.addVertex();
	    vertex1.addEdge("type1", vertex2, properties);
	    vertex2.addEdge("type2", vertex3, properties);
	} finally {
	    graph.rollback();
	}
    }

    @Test(expected = DuctileDBUniqueConstraintViolationException.class)
    public void testEdgeTypeUniqueConstraintViolation() {
	assertNull(schemaManager.getPropertyDefinition(ElementType.EDGE, "property"));
	PropertyDefinition<String> definition = new PropertyDefinition<>(ElementType.EDGE, "property", String.class,
		UniqueConstraint.TYPE);
	schemaManager.defineProperty(definition);

	Map<String, Object> properties = new HashMap<>();
	properties.put("property", "value");
	try {
	    DuctileDBVertex vertex1 = graph.addVertex();
	    DuctileDBVertex vertex2 = graph.addVertex();
	    DuctileDBVertex vertex3 = graph.addVertex();
	    vertex1.addEdge("type1", vertex2, properties);
	    vertex2.addEdge("type1", vertex3, properties);
	} finally {
	    graph.rollback();
	}
    }

    @Test
    public void testInitialTypeDefinitions() {
	Iterable<String> definedTypes = schemaManager.getDefinedTypes();
	assertNotNull(definedTypes);
	assertFalse(definedTypes.iterator().hasNext());
    }

    @Test
    public void testAddGetAndRemoveTypeDefinitions() {
	PropertyDefinition<String> propertyDefinition = new PropertyDefinition<>(ElementType.VERTEX, "property",
		String.class, UniqueConstraint.GLOBAL);
	schemaManager.defineProperty(propertyDefinition);
	assertNull(schemaManager.getTypeDefinition(ElementType.VERTEX, "type"));
	Set<String> propertyKeys = new HashSet<>();
	propertyKeys.add("property");
	schemaManager.defineType(ElementType.VERTEX, "type", propertyKeys);
	Set<String> readPropertyKeys = schemaManager.getTypeDefinition(ElementType.VERTEX, "type");
	assertNotNull(readPropertyKeys);
	assertEquals(propertyKeys, readPropertyKeys);
	schemaManager.removeTypeDefinition(ElementType.VERTEX, "type");
	assertNull(schemaManager.getTypeDefinition(ElementType.VERTEX, "type"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDefineTypeThrowsIllegalArgumentExceptionForNullElementType() {
	PropertyDefinition<String> propertyDefinition = new PropertyDefinition<>(ElementType.VERTEX, "property",
		String.class, UniqueConstraint.GLOBAL);
	schemaManager.defineProperty(propertyDefinition);
	Set<String> propertyKeys = new HashSet<>();
	propertyKeys.add("property");
	schemaManager.defineType(null, "type", propertyKeys);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDefineTypeThrowsIllegalArgumentExceptionForNullTypeName() {
	PropertyDefinition<String> propertyDefinition = new PropertyDefinition<>(ElementType.VERTEX, "property",
		String.class, UniqueConstraint.GLOBAL);
	schemaManager.defineProperty(propertyDefinition);
	Set<String> propertyKeys = new HashSet<>();
	propertyKeys.add("property");
	schemaManager.defineType(ElementType.VERTEX, null, propertyKeys);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDefineTypeThrowsIllegalArgumentExceptionForEmtpyTypeName() {
	PropertyDefinition<String> propertyDefinition = new PropertyDefinition<>(ElementType.VERTEX, "property",
		String.class, UniqueConstraint.GLOBAL);
	schemaManager.defineProperty(propertyDefinition);
	Set<String> propertyKeys = new HashSet<>();
	propertyKeys.add("property");
	schemaManager.defineType(ElementType.VERTEX, "", propertyKeys);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDefineTypeThrowsIllegalArgumentExceptionForNullPropertyKeys() {
	schemaManager.defineType(ElementType.VERTEX, "type", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDefineTypeThrowsIllegalArgumentExceptionForEmtpyPropertyKeys() {
	schemaManager.defineType(ElementType.VERTEX, "type", new HashSet<>());
    }

    @Test(expected = DuctileDBSchemaException.class)
    public void testDefineTypeThrowsDuctileDBSchemaExceptionForEmtpyUnknownPropertyKey() {
	HashSet<String> propertyKeys = new HashSet<>();
	propertyKeys.add("unknownProperty");
	schemaManager.defineType(ElementType.VERTEX, "type", propertyKeys);
    }
}
